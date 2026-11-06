//! End-to-end: build an image with Writer, then walk it back with Reader
//! and compare against the source tree.

use std::collections::BTreeMap;
use std::io::Cursor;

use erofs_builder::{CreateOptions, InodeMeta, Reader, Writer};

const BS: usize = 4096;

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Dir(BTreeMap<String, Node>),
    File(Vec<u8>, bool),
    Symlink(String),
}

fn dir(entries: &[(&str, Node)]) -> Node {
    Node::Dir(
        entries
            .iter()
            .map(|(n, v)| ((*n).to_string(), v.clone()))
            .collect(),
    )
}

async fn build(tree: &Node) -> Vec<u8> {
    let opts = CreateOptions {
        block_size: BS,
        ..Default::default()
    };
    let mut w = Writer::new(Cursor::new(Vec::new()), opts).await.unwrap();
    fn add<'a>(
        w: &'a mut Writer<Cursor<Vec<u8>>>,
        path: &'a str,
        n: &'a Node,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            match n {
                Node::Dir(children) => {
                    w.mkdir(path, InodeMeta::dir(0o555)).await.unwrap();
                    for (name, c) in children {
                        add(w, &format!("{path}/{name}"), c).await;
                    }
                }
                Node::File(data, exec) => {
                    let meta = InodeMeta::reg(if *exec { 0o555 } else { 0o444 });
                    let mut r = data.as_slice();
                    w.add_file(path, meta, data.len() as u64, &mut r)
                        .await
                        .unwrap();
                }
                Node::Symlink(target) => {
                    w.symlink(path, target.as_bytes(), InodeMeta::symlink())
                        .await
                        .unwrap();
                }
            }
        })
    }
    add(&mut w, "", tree).await;
    let cursor = w.finish().await.unwrap();
    cursor.into_inner()
}

fn source_tree() -> Node {
    let mut many = BTreeMap::new();
    for i in 0..300 {
        let data = vec![(i % 251) as u8; (i % 7) * 100];
        many.insert(
            format!("entry-{i:04}-with-a-longish-name"),
            Node::File(data, false),
        );
    }
    dir(&[
        (
            "sub",
            dir(&[
                ("run.sh", Node::File(b"#!/bin/sh\n".to_vec(), true)),
                (
                    "nested",
                    dir(&[("deep.txt", Node::File(b"deep".to_vec(), false))]),
                ),
                ("link", Node::Symlink("deep.txt".to_string())),
            ]),
        ),
        ("empty.bin", Node::File(Vec::new(), false)),
        ("inline.txt", Node::File(b"inline".to_vec(), false)),
        (
            "streamed.bin",
            Node::File((0..10_000u32).map(|i| (i % 251) as u8).collect(), false),
        ),
        ("big-link", Node::Symlink("sub/nested/deep.txt".to_string())),
        ("many", Node::Dir(many)),
    ])
}

#[tokio::test]
async fn reader_round_trips_tree() {
    let tree = source_tree();
    let img = build(&tree).await;
    let tmp = tempfile::tempdir().unwrap();
    let img_path = tmp.path().join("img.erofs");
    std::fs::write(&img_path, &img).unwrap();

    let mut r = Reader::open(&img_path).await.unwrap();
    fn walk<'a>(
        r: &'a mut Reader,
        nid: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Node> + 'a>> {
        Box::pin(async move {
            let mut entries = r.dirents(nid).await.unwrap();
            // reader returns stored order; sort for comparison like a consumer would
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            let mut children = BTreeMap::new();
            for e in entries {
                let node = match e.kind {
                    erofs_builder::FileType::Dir => walk(r, e.nid).await,
                    erofs_builder::FileType::Symlink => {
                        let mut target = Vec::new();
                        r.read_content(e.nid, &mut |chunk| target.extend_from_slice(chunk))
                            .await
                            .unwrap();
                        Node::Symlink(String::from_utf8(target).unwrap())
                    }
                    _ => {
                        let st = r.stat(e.nid).await.unwrap();
                        let exec = st.mode & 0o111 != 0;
                        let mut data = Vec::with_capacity(st.size as usize);
                        let n = r
                            .read_content(e.nid, &mut |chunk| data.extend_from_slice(chunk))
                            .await
                            .unwrap();
                        assert_eq!(n, st.size);
                        Node::File(data, exec)
                    }
                };
                children.insert(e.name, node);
            }
            Node::Dir(children)
        })
    }
    let root_nid = r.root_nid();
    let root = walk(&mut r, root_nid).await;
    match (&tree, &root) {
        (Node::Dir(a), Node::Dir(b)) => {
            // the source root has no "." / ".." entries to compare
            assert_eq!(a, b);
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn dirents_are_name_sorted_test() {
    // writer inserts out of byte order; dirents must come back sorted
    let tree = dir(&[
        ("z-first-added", Node::File(b"z".to_vec(), false)),
        ("a", Node::File(b"a".to_vec(), false)),
        ("m", Node::File(b"m".to_vec(), false)),
    ]);
    let img = build(&tree).await;
    let tmp = tempfile::tempdir().unwrap();
    let img_path = tmp.path().join("img.erofs");
    std::fs::write(&img_path, &img).unwrap();
    let mut r = Reader::open(&img_path).await.unwrap();
    let names: Vec<String> = r
        .dirents(r.root_nid())
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert_eq!(names, ["a", "m", "z-first-added"]);
}

#[tokio::test]
async fn bad_magic_is_rejected_test() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("junk");
    std::fs::write(&path, vec![0u8; 4096]).unwrap();
    assert!(Reader::open(&path).await.is_err());
}

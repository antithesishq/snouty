use snouty::testutils::MockApiServer;

fn main() {
    let server = MockApiServer::start();
    let url = server.url();

    eprintln!("Mock Antithesis API listening on {url}");
    eprintln!();
    eprintln!("  export ANTITHESIS_BASE_URL={url}");
    eprintln!("  export ANTITHESIS_USERNAME=mock");
    eprintln!("  export ANTITHESIS_PASSWORD=mock");
    eprintln!("  export ANTITHESIS_TENANT=mock");

    server.wait();
}

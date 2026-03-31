use snouty::testutils::MockApiServer;

fn main() {
    let server = MockApiServer::start();
    let url = server.url();
    let token = server.token();

    eprintln!("Mock Antithesis API listening on {url}");
    eprintln!();
    eprintln!("  export ANTITHESIS_BASE_URL={url}");
    eprintln!("  export ANTITHESIS_API_KEY={token}");
    eprintln!("  export ANTITHESIS_TENANT=mock");

    server.wait();
}

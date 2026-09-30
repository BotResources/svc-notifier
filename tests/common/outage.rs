// Storage outage fixture: the one accommodation the harness does not provide.
use std::process::Command;

// A frozen storage: the Postgres container is paused, so connections stay open
// and writes block — the shape a real storage stall takes, not a torn-down
// socket. Dropping the guard resumes it, even when the scenario panics.
// GAP: the harness cannot pause storage; this drives the docker CLI against the
// container publishing the test Postgres port (docker-compose.test.yml).
pub struct PausedPostgres {
    container: String,
}

impl PausedPostgres {
    pub fn pause() -> Self {
        let container = postgres_container();
        run_docker(&["pause", &container]);
        Self { container }
    }
}

impl Drop for PausedPostgres {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["unpause", &self.container])
            .output();
    }
}

fn postgres_container() -> String {
    let port = postgres_host_port();
    docker_ps_first_postgres(&["--filter", &format!("publish={port}")]).unwrap_or_else(|| {
        panic!(
            "no running postgres container publishes port {port} — the storage outage scenario \
             needs docker-compose.test.yml (a native Postgres cannot be paused)"
        )
    })
}

fn docker_ps_first_postgres(filters: &[&str]) -> Option<String> {
    let mut args = vec!["ps"];
    args.extend_from_slice(filters);
    args.extend_from_slice(&["--format", "{{.Names}}\t{{.Image}}"]);
    let output = Command::new("docker")
        .args(&args)
        .output()
        .expect("docker ps failed — the outage scenario needs the docker CLI");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find(|line| line.contains("postgres"))
        .map(|line| line.split('\t').next().unwrap_or_default().to_string())
}

fn postgres_host_port() -> String {
    let _ = dotenvy::from_filename(".env.test");
    std::env::var("DATABASE_URL_OWNER")
        .ok()
        .and_then(|url| {
            let authority = url.rsplit('@').next()?.to_string();
            let port = authority.split('/').next()?.split(':').nth(1)?.to_string();
            (!port.is_empty()).then_some(port)
        })
        .unwrap_or_else(|| "5432".to_string())
}

fn run_docker(args: &[&str]) {
    let output = Command::new("docker")
        .args(args)
        .output()
        .expect("docker command failed to start");
    assert!(
        output.status.success(),
        "docker {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

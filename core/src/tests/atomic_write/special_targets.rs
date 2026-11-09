use super::*;
use std::os::unix::fs::FileTypeExt as _;

fn file_type(path: &Path) -> std::fs::FileType {
    std::fs::symlink_metadata(path).unwrap().file_type()
}

/// Make a pipe at `path` with `mkfifo`; false when the tool is not there.
fn make_fifo(path: &Path) -> bool {
    std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .is_ok_and(|status| status.success())
}

async fn write_to(path: &Path) -> crate::error::Result<()> {
    crate::io::quads_stream_to_vortex_file(
        quad_stream(modular_quads(12, 3, 4)),
        path,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
}

/// `/dev/null` takes the store and remains a character device.
#[tokio::test]
async fn test_a_character_device_is_written_through_not_replaced() {
    let device = Path::new("/dev/null");
    write_to(device).await.unwrap();
    assert!(file_type(device).is_char_device(), "/dev/null was replaced");
}

/// A pipe is written into while a reader drains it: the reader gets a
/// whole store, and the pipe is still a pipe.
#[tokio::test]
async fn test_a_pipe_is_written_through_while_a_reader_drains_it() {
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("pipe");
    if !make_fifo(&fifo) {
        eprintln!("skipped: mkfifo is not available");
        return;
    }
    let (sender, received) = std::sync::mpsc::channel();
    let reader_path = fifo.clone();
    std::thread::spawn(move || {
        use std::io::Read as _;
        let mut bytes = Vec::new();
        let result = std::fs::File::open(&reader_path)
            .and_then(|mut pipe| pipe.read_to_end(&mut bytes))
            .map(|_| bytes);
        let _ = sender.send(result);
    });

    write_to(&fifo).await.unwrap();

    let bytes = received
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the reader saw the end of the stream")
        .unwrap();
    assert!(file_type(&fifo).is_fifo(), "the pipe was replaced");
    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 12);
    assert_eq!(entries(dir.path()), vec![fifo]);
}

/// A socket cannot take a store: the write is refused with an error that
/// names the path and the kinds of file a store can go to, before any
/// input is read, and the socket stays.
#[tokio::test]
async fn test_other_special_files_are_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("socket");
    let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let polled = Arc::new(AtomicBool::new(false));

    let error = crate::io::quads_stream_to_vortex_file(
        recording_stream(polled.clone()),
        &socket,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .expect_err("a socket cannot be replaced by a store");

    assert!(matches!(error, VortexRdfError::Io(_)), "{error}");
    assert!(
        error.to_string().contains(&format!("{socket:?}"))
            && error
                .to_string()
                .contains("not a regular file, a character device or a pipe"),
        "the error must name the path and say why: {error}"
    );
    assert!(!polled.load(Ordering::SeqCst), "the input was read");
    assert!(file_type(&socket).is_socket(), "the socket was replaced");
    assert_eq!(entries(dir.path()), vec![socket]);
}

/// A store opened from a file is never written into a pipe that has
/// taken the file's path since: its compaction is refused as replaced
/// since the open, and the pipe is left alone. The compaction runs on a
/// thread of its own and is waited for at most 10 s, since one that
/// opened the pipe, which has no reader, would block there.
#[tokio::test]
async fn test_compaction_refuses_a_path_turned_into_a_pipe_since_the_store_opened() {
    let (dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
    let tailed = VortexRdfStore::from_file(&path)
        .await
        .unwrap()
        .add_quads(appended_quads(2))
        .await
        .unwrap();
    let fifo = dir.path().join("pipe");
    if !make_fifo(&fifo) {
        eprintln!("skipped: mkfifo is not available");
        return;
    }
    std::fs::rename(&fifo, &path).unwrap();

    let (sender, compacted) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _ = sender.send(runtime.block_on(tailed.compact()).err());
    });
    let error = compacted
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the compaction returns within 10 s")
        .expect("a pipe is not the file the store opened");
    assert!(
        matches!(&error, VortexRdfError::InvalidOperation(m)
                if m.contains("was replaced since this store opened it")),
        "{error}"
    );
    assert!(file_type(&path).is_fifo(), "the pipe was replaced");
    assert_eq!(entries(dir.path()), vec![path]);
}

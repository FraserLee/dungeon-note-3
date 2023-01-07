// MINIMUM FAILING SAMPLE : run with `cargo run -- <path to some file>`
// GOAL: to send an SSE event with warp whenever a file changes
// CURRENT: I've got this set up to do three things:
// 1. WORKING: Send, receive, and display an SSE event every 3 seconds
// 2. WORKING: Asynchronously watch a directory for changes, outputting to the console when a change is detected
// 3. FAILING: Send an SSE event when a change is detected

use std::convert::Infallible;
use std::path::Path;
use std::time::Duration;

use async_stream::stream;
use futures::{ channel::mpsc::{channel, Receiver}, SinkExt, };
use futures_util::stream::StreamExt;
use warp::{sse, Filter};

use notify::{ Event, RecommendedWatcher, RecursiveMode, Watcher, Config };

// basic test web page, adds a line to the body each time an SSE event is received
const INDEX_HTML: &str = r#"
<!DOCTYPE HTML>
<html>
<body>
  <h1>Warp SSE on File Change Demo</h1>
  <script> 
      var tickSource = new EventSource("/tick");
      var fileChangeSource = new EventSource("/file_change");

      tickSource.onmessage = (event) => {
          document.body.innerHTML += "<p>tick event</p>";
      };

      fileChangeSource.onmessage = (event) => {
          document.body.innerHTML += "<p>file change event</p>";
      };
  </script>
</body>
</html>
"#;


// simple SSE event
fn sse_event() -> Result<sse::Event, Infallible> { Ok(sse::Event::default().data("")) }

// from the example in the notify crate for working with tokio
// https://github.com/notify-rs/notify/blob/e375fcefd23edd23e7138d8b3a97a721d6b7bbca/examples/async_monitor.rs#L22
fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<Event>>)> {
    let (mut tx, rx) = channel(1);

    let watcher = RecommendedWatcher::new(move |res| {
        futures::executor::block_on(async {
            tx.send(res).await.unwrap();
        })
    }, Config::default())?;

    Ok((watcher, rx))
}



// locally watch a file for changes
async fn local() {
    let (mut watcher, mut rx) = async_watcher().unwrap();

    watcher.watch(
            Path::new(&*std::env::args().nth(1).unwrap()), 
            RecursiveMode::Recursive
        ).unwrap();

    loop {
        if let Some(_) = rx.next().await {
            println!("local context: file changed");
        }
    }
}



// host a webpage with 2 SSE endpoints
async fn webpage() {
    let index = warp::path::end().map(|| warp::reply::html(INDEX_HTML));

    // send an sse event on /tick every 3 seconds
    let tick_sse  = warp::path("tick").and(warp::get()).map(|| {

        let stream = stream! {
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                println!("sending tick event");
                yield sse_event();
            }
        };

        warp::sse::reply(warp::sse::keep_alive().stream(stream))

    });

    // sse event on /file_change every time the file changes
    // BROKEN: this is the bit that doesn't seem to work, despite nearly 
    // identical code working in the local() function to watch the file, and the
    // stream! macro working in the tick_sse function.
    let file_change_sse = warp::path("file_change").and(warp::get()).map(|| {

        let (mut watcher, mut rx) = async_watcher().unwrap();

        watcher.watch(
                Path::new(&*std::env::args().nth(1).unwrap()), 
                RecursiveMode::Recursive
            ).unwrap();

        let stream = stream! {
            while let Some(_) = rx.next().await {
                println!("sending file change event");
                yield sse_event();
            }
        };

        warp::sse::reply(stream)
    });

    // -- run server -----------------------------------------------------------

    println!(" -- serving at http://localhost:3000 --");
    println!(" -------- press ctrl-c to stop --------");

    warp::serve(index.or(tick_sse).or(file_change_sse)).run(([127, 0, 0, 1], 3000)).await;
}


#[tokio::main]
async fn main() { tokio::join!(webpage(), local()); }

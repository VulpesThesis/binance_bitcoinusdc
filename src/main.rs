use url::Url;
use futures_util::stream::StreamExt;
use chrono::Utc;
use serde::{Deserialize, Serialize}; // Added Serialize for sending data out
use std::sync::Arc;                  // Needed for Layer 3 State
use tokio::sync::broadcast;

// cLIENT
use yawc::{WebSocket as YawcWS, frame::OpCode}; 

// Server
use axum::{
    extract::{
        ws::{Message, WebSocket as AxumWS, WebSocketUpgrade}, 
        State
    },
    response::IntoResponse,
    routing::get,
    Router,
};

const CHANNEL_CAPACITY: usize = 100;

/// Deserialize Binance trade data for incoming messages
/// Serialize is needed to send data out to clients
#[derive(Deserialize,Serialize, Clone, Debug)]
struct BinanceTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This is the broadcast channel to share trade data with multiple consumers
    // tx is the sender (Binance), _rx is the receiver (clients)
    // Holding 100 messages in the buffer
    let (tx, _rx) = broadcast::channel::<BinanceTrade>(CHANNEL_CAPACITY);

    // We need a clone of the sender because of owning rules in Rust?
    // "&" is for borrowing (Should be returned when function ends) "clone()" creates a new instance
    // We need it because of the async nature of WebSocket connections
    let tx_binance_clone = tx.clone();

    // Spawn a new asynchronous task to handle the Binance WebSocket connection
    // According to documentation, this will run in a new thread or on the same thread, depending on the runtime configuration
    tokio::spawn(async move {
        // We create the URL for the Binance WebSocket stream inside the async block so that we don't have to clone it
        // Cant use ? here because we are in an async block (contains no return type, it is contained in the main function)
        // unwrap() will panic if there is an error, crashing the task but not the whole program
        let url = Url::parse("wss://stream.binance.com:9443/ws/btcusdt@trade").unwrap();
        let mut counter: u64 = 0;
        loop {
            counter += 1;
            println!("Connecting to Binance WebSocket... Attempt {}", counter);
            // Only runs if "Ok", this way we can handle connection errors gracefully
            // Was using ifs before, but match is the optimal way
            match YawcWS::connect(url.clone()).await {
                Ok(mut ws) => {
                    println!("Connected to Binance WebSocket stream.");
                    // Process incoming trade frames
                    let _ = process_trades(&mut ws, tx_binance_clone.clone()).await;
                }
                Err(_) =>{
                    println!("Failed to connect to Binance WebSocket. Retrying in 5 seconds...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // Start API
    trade_api(tx).await;

    Ok(())
}

async fn process_trades(ws: &mut YawcWS, tx: broadcast::Sender<BinanceTrade>) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(frame) = ws.next().await {
        match frame.opcode {
            OpCode::Text => {
                match std::str::from_utf8(&frame.payload) {
                    Ok(text) => {
                        match serde_json::from_str::<BinanceTrade>(text) {
                            Ok(trade) => {
                                // Send the trade data to all subscribers
                                // still need to update to use the struct
                                match tx.send(trade.clone()) {
                                    Ok(_) => {
                                        println!("Trade data sent to subscribers: {:?}", trade);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to send trade data: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to deserialize trade data: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to parse text frame: {}", e);
                    }
                }
            }
            OpCode::Close => {
                println!("Connection closed by server.");
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Axum api for clients to connect and receive trade data
async fn trade_api(tx: broadcast::Sender<BinanceTrade>) {
    // Shared state needs to be thread-safe
    // We dont use .clone() here because we want to share the same instance, because alot of data will be sent
    let app_state = Arc::new(tx);

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("🚀 Server started at http://localhost:3000/ws");
    
    axum::serve(listener, app).await.unwrap();
}

/// The Handshake handler
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<broadcast::Sender<BinanceTrade>>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_client(socket, state))
}

async fn handle_client(mut socket: AxumWS, tx: Arc<broadcast::Sender<BinanceTrade>>) {
    let mut rx = tx.subscribe(); // Subscribe to the broadcast
    while let Ok(trade) = rx.recv().await {
        if let Ok(msg) = serde_json::to_string(&trade) {
            if socket.send(Message::Text(msg.into())).await.is_err() {
                break; // Client disconnected
            }
        }
    }
}


/// USED WEBSOCAT FOR TESTING:
/// websocat -v ws://127.0.0.1:3000/ws
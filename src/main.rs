use url::Url;
use futures_util::stream::StreamExt;
use chrono::Utc;
use serde::Deserialize;
use yawc::{WebSocket, frame::OpCode};

#[derive(Deserialize, Debug)]
struct BinanceTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")] // 'p' is the specific price of this trade
    price: String,
    #[serde(rename = "q")] // 'q' is the quantity
    quantity: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // this is the websocket endpoint for Binance's BTC/USDT trade stream
    // I was previously using ticker stream but this gives real price per trade not 24h avg
    // Not sure if i should use aggregate trade stream instead
    let url = Url::parse("wss://stream.binance.com:9443/ws/btcusdt@trade")?;

    println!("Connecting to Binance trade stream...");
    let mut ws = WebSocket::connect(url).await?;
    println!("Connected! Watching every single BTC/USDT trade...\n");

    while let Some(frame) = ws.next().await {
        match frame.opcode {
            OpCode::Text => {
                if let Ok(text) = std::str::from_utf8(&frame.payload) {
                    if let Ok(trade) = serde_json::from_str::<BinanceTrade>(text) {
                        println!(
                            "[{}] {} Trade: ${} (Qty: {})", 
                            Utc::now().format("%H:%M:%S%.3f"), // Added milliseconds
                            trade.symbol, 
                            trade.price,
                            trade.quantity
                        );
                    }
                }
            }
            OpCode::Close => {
                println!("Connection closed.");
                break;
            }
            // Should I handle ping/pong frames here? I think websocket library does it automatically
            _ => {}
        }
    }
    Ok(())
}
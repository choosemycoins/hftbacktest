use algo::gridtrading;
use hftbacktest::{
    live::{
        BotError,
        Instrument,
        LiveBot,
        LiveBotBuilder,
        LoggingRecorder,
        ipc::iceoryx::IceoryxUnifiedChannel,
    },
    prelude::{Bot, ErrorKind, HashMapMarketDepth, Value},
};
use tracing::error;

#[path = "common/algo.rs"]
mod algo;

const ORDER_PREFIX: &str = "prefix";

fn prepare_live() -> LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> {
    let mut hbt = LiveBotBuilder::new()
        .register(Instrument::new(
            "binancefutures",
            "SOLUSDT",
            0.001,
            1.0,
            HashMapMarketDepth::new(0.001, 1.0),
            0,
        ))
        // The connector the error came from — see `gridtrading_live_bybit.rs` for why it is
        // worth branching on rather than only logging.
        .error_handler(|connector, error| {
            match error.kind {
                ErrorKind::ConnectionInterrupted => {
                    error!(%connector, "ConnectionInterrupted");
                }
                ErrorKind::CriticalConnectionError => {
                    error!(%connector, "CriticalConnectionError");
                }
                ErrorKind::OrderError => {
                    let error = error.value();
                    match error {
                        Value::String(err) => {
                            error!(%connector, ?err, "OrderError");
                        }
                        Value::Map(err) => {
                            error!(%connector, ?err, "OrderError");
                        }
                        _ => {}
                    }
                }
                ErrorKind::Custom(errno) => {
                    if errno == 1000 {
                        // Aborts the connection.
                        return Err(BotError::Custom("UserStreamError".to_string()));
                    }
                }
            }
            Ok(())
        })
        .build()
        .unwrap();

    hbt
}

fn main() {
    tracing_subscriber::fmt::init();

    let mut hbt = prepare_live();

    let relative_half_spread = 0.0005;
    let relative_grid_interval = 0.0005;
    let grid_num = 10;
    let min_grid_step = 0.001; // tick size
    let skew = relative_half_spread / grid_num as f64;
    let order_qty = 1.0;
    let max_position = grid_num as f64 * order_qty;

    let mut recorder = LoggingRecorder::new();
    gridtrading(
        &mut hbt,
        &mut recorder,
        relative_half_spread,
        relative_grid_interval,
        grid_num,
        min_grid_step,
        skew,
        order_qty,
        max_position,
    )
    .unwrap();
    hbt.close().unwrap();
}

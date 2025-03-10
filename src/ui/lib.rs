use native_tls::TlsStream;
use std::collections::HashMap;
use std::net::TcpStream;
use std::time::Instant;

use crate::protocol;
use crate::ui::{map::CustomMapMemory, places, plugins};

use protocol::protocol_body::compression::Compression;

use egui::Context;
use walkers::{HttpOptions, HttpTiles, Map, Tiles};

use super::plugins::ClickWatcher;
use super::provider::Provider;

/// Returns the HTTP options for the application.
///
/// # Returns
/// `HttpOptions` - The HTTP options configured for the application.
fn http_options() -> HttpOptions {
    HttpOptions {
        cache: if cfg!(target_os = "android") || std::env::var("NO_HTTP_CACHE").is_ok() {
            None
        } else {
            Some(".cache".into())
        },
        ..Default::default()
    }
}

/// Returns a map of available tile providers.
///
/// # Arguments
/// * `egui_ctx` - The Egui context.
///
/// # Returns
/// `HashMap<Provider, Box<dyn Tiles + Send>>` - A map of available tile providers.
fn providers(egui_ctx: Context) -> HashMap<Provider, Box<dyn Tiles + Send>> {
    let mut providers: HashMap<Provider, Box<dyn Tiles + Send>> = HashMap::default();

    providers.insert(
        Provider::OpenStreetMap,
        Box::new(HttpTiles::with_options(
            walkers::sources::OpenStreetMap,
            http_options(),
            egui_ctx.to_owned(),
        )),
    );

    let mapbox_access_token = std::option_env!("MAPBOX_ACCESS_TOKEN");

    if let Some(token) = mapbox_access_token {
        providers.insert(
            Provider::MapboxStreets,
            Box::new(HttpTiles::with_options(
                walkers::sources::Mapbox {
                    style: walkers::sources::MapboxStyle::Streets,
                    access_token: token.to_string(),
                    high_resolution: false,
                },
                http_options(),
                egui_ctx.to_owned(),
            )),
        );
        providers.insert(
            Provider::MapboxSatellite,
            Box::new(HttpTiles::with_options(
                walkers::sources::Mapbox {
                    style: walkers::sources::MapboxStyle::Satellite,
                    access_token: token.to_string(),
                    high_resolution: true,
                },
                http_options(),
                egui_ctx.to_owned(),
            )),
        );
    }

    providers
}

/// Main application structure.
pub struct MyApp<'a> {
    providers: HashMap<Provider, Box<dyn Tiles + Send>>,
    selected_provider: Provider,
    map_memory: CustomMapMemory,
    click_watcher: plugins::ClickWatcher,
    tcp_stream: &'a mut TlsStream<TcpStream>,
}

impl<'a> MyApp<'a> {
    /// Creates a new instance of `MyApp`.
    ///
    /// # Arguments
    /// * `egui_ctx` - The Egui context.
    ///
    /// # Returns
    /// `MyApp` - A new instance of `MyApp`.
    pub fn new(
        egui_ctx: Context,
        server: &'a mut TlsStream<TcpStream>,
        compression: Option<Compression>,
    ) -> Self {
        let _ = compression;

        egui_extras::install_image_loaders(&egui_ctx);

        Self {
            providers: providers(egui_ctx.to_owned()),
            selected_provider: Provider::OpenStreetMap,
            map_memory: CustomMapMemory::default(),
            click_watcher: ClickWatcher::new(CustomMapMemory::new()),
            tcp_stream: server,
        }
    }
}

impl eframe::App for MyApp<'_> {
    /// Updates the application state.
    ///
    /// # Arguments
    /// * `ctx` - The Egui context.
    /// * `_frame` - The Eframe frame.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let rimless = egui::Frame {
            fill: ctx.style().visuals.panel_fill,
            ..Default::default()
        };

        let start = Instant::now();

        egui::CentralPanel::default()
            .frame(rimless)
            .show(ctx, |ui| {
                let my_position = places::facultad_de_ingenieria();

                let tiles = match self.providers.get_mut(&self.selected_provider) {
                    Some(provider) => provider.as_mut(),
                    None => {
                        eprintln!("Selected provider not found");
                        return;
                    }
                };

                let map = Map::new(Some(tiles), &mut self.map_memory.map_memory, my_position);

                let map = map
                    .with_plugin(plugins::places())
                    .with_plugin(plugins::CustomShapes {})
                    .with_plugin(&mut self.click_watcher);

                ui.add(map);

                {
                    use crate::ui::windows::*;

                    let elapsed = start.elapsed();

                    zoom(ui, &mut self.map_memory.map_memory);
                    //go_to_my_position(ui, &mut self.map_memory.map_memory);
                    self.click_watcher.show_position(ui);
                    let _ = self.click_watcher.clicked_airport_info(
                        ui,
                        self.tcp_stream,
                        elapsed.as_secs(),
                    );
                    self.click_watcher
                        .clicked_plane_info(ui, self.tcp_stream, elapsed.as_secs());
                }
            });
    }
}

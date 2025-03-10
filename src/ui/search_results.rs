// Enum to differentiate search types

use std::{collections::HashMap, net::TcpStream};

use egui::{Align2, Color32, RichText, Ui, Window};
use native_tls::TlsStream;

use crate::receiver::message::Message::ReplyMessage;
use crate::receiver::response_message::ResponseMessage;

use crate::{
    errors::error_types::ErrorTypes,
    protocol::{
        protocol_body::{compression::Compression, query_flags::QueryFlags},
        protocol_notations::consistency,
        protocol_writer::Protocol,
    },
    receiver::result_response::ResultResponse,
    server::query_execute::conect_server,
};

use super::windows::{get_planes_positions, make_query, Airport, AppState};

const COMPRESSION: Option<Compression> = None;

#[derive(Clone)]
pub enum SearchType {
    Arrivals,
    Departures,
}

// Struct to encapsulate search results
#[derive(Default, Clone, Debug)]
pub struct SearchResults {
    pub arrivals: Vec<Vec<String>>,   // Results for arrivals
    pub departures: Vec<Vec<String>>, // Results for departures
}

impl AppState {
    pub fn new(_server: TcpStream) -> Self {
        Self {
            search_date: String::new(),
            last_searched_date: None,
            search_results: Some(SearchResults::default()),
            is_searching: false,
            search_error: None,
            planes_positions: HashMap::new(),
        }
    }

    pub fn search_ui(
        &mut self,
        ui: &mut Ui,
        airport: &Airport,
        server: &mut TlsStream<TcpStream>,
        enlapsed: u64,
    ) {
        Window::new("Airport Search")
            .collapsible(false)
            .resizable(true)
            .title_bar(false)
            .anchor(Align2::LEFT_TOP, [10., 10.])
            .default_size([450.0, 350.0])
            .show(ui.ctx(), |ui| {
                ui.vertical(|ui| {
                    ui.push_id("information section", |ui| {
                        ui.label(RichText::new(airport.name.to_string()).size(18.0));
                        if self.search_results.is_none() {
                            let results = match (
                                make_query(
                                    airport,
                                    &self.search_date,
                                    server,
                                    SearchType::Arrivals,
                                ),
                                make_query(
                                    airport,
                                    &self.search_date,
                                    server,
                                    SearchType::Departures,
                                ),
                            ) {
                                (Ok(arrivals), Ok(departures)) => SearchResults {
                                    arrivals,
                                    departures,
                                },
                                _ => {
                                    self.search_error =
                                        Some("Failed to fetch search results".to_string());
                                    return;
                                }
                            };
                            self.search_results = Some(results);
                        } else if enlapsed as u32 % 5 == 0 {
                            if let Some(results) = &mut self.search_results {
                                match make_query(
                                    airport,
                                    &self.search_date,
                                    server,
                                    SearchType::Arrivals,
                                ) {
                                    Ok(arrivals) => results.arrivals = arrivals,
                                    Err(_) => {
                                        self.search_error =
                                            Some("Failed to fetch arrivals".to_string());
                                        return;
                                    }
                                }
                                match make_query(
                                    airport,
                                    &self.search_date,
                                    server,
                                    SearchType::Departures,
                                ) {
                                    Ok(departures) => results.departures = departures,
                                    Err(_) => {
                                        self.search_error =
                                            Some("Failed to fetch departures".to_string());
                                        return;
                                    }
                                }
                            }
                        }

                        if let Some(results) = &self.search_results {
                            self.planes_positions = get_planes_positions(results);
                            ui.separator();
                            {
                                ui.collapsing(RichText::new("Arrivals").size(18.0), |ui| {
                                    for arrival in &results.arrivals {
                                        let mut text = String::new();
                                        for a in &arrival[..5] {
                                            text.push_str(a);
                                            text.push_str(" | ");
                                        }
                                        ui.label(RichText::new(text).size(18.0));
                                    }
                                });
                            }

                            {
                                ui.collapsing(RichText::new("Departures").size(18.0), |ui| {
                                    for departure in &results.departures {
                                        let mut text = String::new();
                                        for a in &departure[..4] {
                                            text.push_str(a);
                                            text.push_str(" | ");
                                        }
                                        text.push_str(&departure[5]);
                                        text.push_str(" | ");
                                        ui.label(RichText::new(text).size(18.0));
                                    }
                                });
                            }
                        }

                        if let Some(error) = &self.search_error {
                            ui.colored_label(Color32::RED, error);
                        }
                    });
                });
            });
    }

    pub fn perform_search(
        &self,
        date: &str,
        search_type: SearchType,
    ) -> Result<Vec<Vec<String>>, String> {
        match search_type {
            SearchType::Arrivals => Ok(vec![vec![format!("Arrival flight on {}", date)]]),
            SearchType::Departures => Ok(vec![vec![format!("Departure flight on {}", date)]]),
        }
    }

    pub fn search_plane_info(
        &self,
        plane_id: &String,
        plane_info: Vec<String>,
        server: &mut TlsStream<TcpStream>,
    ) -> Result<Vec<Vec<String>>, ErrorTypes> {
        let query = format!(
            "SELECT id, status, origin, destination, arrival_time, departure_time, fuel, velocity, height, latitude, longitude  FROM arrivals WHERE id = {} AND destination = {};",
            plane_id, &plane_info[1]
        );

        let mut protocol = Protocol::new();
        protocol.set_compress_algorithm(COMPRESSION);
        protocol.write_query(
            &query,
            consistency::Consistency::One,
            vec![QueryFlags::SkipMetadata],
        )?;
        let message = conect_server(server, Some(protocol), &COMPRESSION);
        match message {
            Ok(ReplyMessage(msg)) => match msg {
                ResponseMessage::Result { kind } => match kind {
                    ResultResponse::Rows { metadata: _, rows } => Ok(rows),
                    _ => Err(ErrorTypes::new(620, "Unexpected message".to_string())),
                },
                _ => Err(ErrorTypes::new(625, "Unexpected message".to_string())),
            },
            Ok(_) => Err(ErrorTypes::new(626, "Unexpected message".to_string())),
            Err(_) => Err(ErrorTypes::new(627, "Error receiving message".to_string())),
        }
    }
}

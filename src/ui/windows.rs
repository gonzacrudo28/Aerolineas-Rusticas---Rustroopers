// use crate::ui::plugins::ImagesPluginData;

use std::{collections::HashMap, net::TcpStream};

use crate::{
    errors::error_types::ErrorTypes,
    protocol::{
        protocol_body::{compression::Compression, query_flags::QueryFlags},
        protocol_notations::consistency::{self, Consistency},
        protocol_writer::*,
    },
    receiver::{
        message::Message::ReplyMessage, response_message::ResponseMessage,
        result_response::ResultResponse,
    },
    server::query_execute::conect_server,
};
use egui::{Align2, RichText, Ui, Window};
use native_tls::TlsStream;
use serde::Deserialize;
use std::f64::consts::PI;
use walkers::MapMemory;

use super::search_results::{SearchResults, SearchType};
const COMPRESSION: Option<Compression> = None;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
/// Represents an airport.
pub struct Airport {
    pub code: String,
    pub lat: String,
    pub lon: String,
    pub name: String,
}
impl Airport {
    pub fn get_airport_code(&self) -> &String {
        &self.code
    }

    pub fn get_latitude(&self) -> Result<f64, ErrorTypes> {
        self.lat
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(681, "Error parsing latitude".to_string()))
    }
    pub fn get_longitude(&self) -> Result<f64, ErrorTypes> {
        self.lon
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(680, "Error parsing longitude".to_string()))
    }

    pub fn distance_to(&self, destination: &Airport) -> Result<f64, ErrorTypes> {
        let lat1 = self
            .lat
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(681, "Error parsing latitude".to_string()))?
            * PI
            / 180.0;
        let lon1 = self
            .lon
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(680, "Error parsing longitude".to_string()))?
            * PI
            / 180.0;
        let lat2 = destination
            .lat
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(681, "Error parsing latitude".to_string()))?
            * PI
            / 180.0;
        let lon2 = destination
            .lon
            .parse::<f64>()
            .map_err(|_| ErrorTypes::new(680, "Error parsing longitude".to_string()))?
            * PI
            / 180.0;
        let dlat = lat2 - lat1;
        let dlon = lon2 - lon1;

        let r = 6371.0;
        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        Ok(r * c)
    }
}

/// Shows the zoom window.
///
/// # Arguments
/// * `ui` - The Egui UI.
/// * `map_memory` - The map memory.
pub fn zoom(ui: &Ui, map_memory: &mut MapMemory) {
    Window::new("Map")
        .collapsible(false)
        .resizable(false)
        .title_bar(false)
        .anchor(Align2::LEFT_BOTTOM, [10., -10.])
        .show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                if ui.button(RichText::new("➕").heading()).clicked() {
                    let _ = map_memory.zoom_in();
                }

                if ui.button(RichText::new("➖").heading()).clicked() {
                    let _ = map_memory.zoom_out();
                }
            });
        });
}

pub fn switch_flight_state(
    server: &mut TlsStream<TcpStream>,
    flight_id: String,
    flight_status: String,
    flight_info: Vec<String>,
) -> Result<(), ErrorTypes> {
    let querys = vec![
        format!(
            "UPDATE arrivals SET status = '{}' WHERE id = {} AND destination = '{}';",
            flight_status, flight_id, &flight_info[1]
        ),
        format!(
            "UPDATE departures SET status = '{}' WHERE id = {} AND origin = '{}';",
            flight_status, flight_id, &flight_info[0]
        ),
    ];
    for query in querys {
        let mut msg = Protocol::new();
        msg.set_compress_algorithm(COMPRESSION);
        msg.write_query(&query, Consistency::Quorum, vec![QueryFlags::SkipMetadata])?;
        let message = conect_server(server, Some(msg), &COMPRESSION)?;

        let msg = match message {
            ReplyMessage(ResponseMessage::Result {
                kind: ResultResponse::Void,
            }) => Ok(()),
            ReplyMessage(_) => Err(ErrorTypes::new(2, "Unexpected message".to_string())),
            _ => Err(ErrorTypes::new(3, "Error receiving message".to_string())),
        };
        msg.as_ref()
            .map_err(|_| ErrorTypes::new(4, "Error receiving message".to_string()))?;
    }
    Ok(())
}
/// Centers the map at the user's position. When map is "detached", show a windows with an option to go back to my position.
///
/// # Arguments
/// * `ui` - The Egui UI.
/// * `map_memory` - The map memory.
pub fn go_to_my_position(ui: &Ui, map_memory: &mut MapMemory) {
    if let Some(position) = map_memory.detached() {
        Window::new("Center")
            .collapsible(false)
            .resizable(false)
            .title_bar(false)
            .anchor(Align2::RIGHT_BOTTOM, [-10., -10.])
            .show(ui.ctx(), |ui| {
                ui.label("map center: ");
                ui.label(format!("{:.04} {:.04}", position.lon(), position.lat()));
                if ui
                    .button(RichText::new("go to the starting point").heading())
                    .clicked()
                {
                    map_memory.follow_my_position();
                }
            });
    }
}

#[derive(Default)]
pub struct AppState {
    pub search_date: String,                // Stores the input date as a string
    pub last_searched_date: Option<String>, // Tracks the last searched date to avoid redundant searches
    pub search_results: Option<SearchResults>, // Stores the results of the most recent search
    pub is_searching: bool,                 // Tracks whether a search is in progress
    pub search_error: Option<String>,       // Stores any error message during search
    pub planes_positions: HashMap<(String, String), Vec<String>>,
}

pub fn get_planes_positions(results: &SearchResults) -> HashMap<(String, String), Vec<String>> {
    let mut planes_positions = HashMap::new();
    if results.arrivals.len() > 1 {
        for arrival in &results.arrivals[1..] {
            let id = arrival[0].clone();
            let status = arrival[1].clone();
            planes_positions.insert((id, status), arrival[2..].to_vec());
        }
    }

    if results.departures.len() > 1 {
        for departure in &results.departures[1..] {
            let id = departure[0].clone();
            let status = departure[1].clone();
            planes_positions.insert((id, status), departure[2..].to_vec());
        }
    }

    planes_positions
}

/// Validates the given date.
///
/// # Arguments
/// * `year` - The year to validate.
/// * `month` - The month to validate.
/// * `day` - The day to validate.
///
/// # Returns
/// `bool` - `true` if the date is valid, `false` otherwise.
///
pub fn is_valid_date(date: &str) -> Result<bool, ErrorTypes> {
    // Simple validation for YYYY-MM-DD format
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        return Ok(false);
    }
    let year = parts[0]
        .parse::<u32>()
        .map_err(|_| ErrorTypes::new(607, "Error parsing year".to_string()))?;
    let month = parts[1]
        .parse::<u32>()
        .map_err(|_| ErrorTypes::new(608, "Error parsing month".to_string()))?;
    let day = parts[2]
        .parse::<u32>()
        .map_err(|_| ErrorTypes::new(609, "Error parsing day".to_string()))?;

    if !(1900..=2100).contains(&year) {
        return Ok(false);
    }

    if !(1..=12).contains(&month) {
        return Ok(false);
    }

    if !(1..=31).contains(&day) {
        return Ok(false);
    }

    Ok(true)
}

pub fn make_query(
    airport: &Airport,
    date: &str,
    server: &mut TlsStream<TcpStream>,
    type_flight: SearchType,
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let query = match type_flight {
        SearchType::Arrivals => format!(
            "SELECT id, status, origin, destination, arrival_time, departure_time, latitude, longitude FROM arrivals WHERE destination = {} AND  arrival_time = {};",
            airport.code, date
        ),
        SearchType::Departures => format!(
            "SELECT id, status, origin, destination, arrival_time, departure_time, latitude, longitude FROM departures WHERE origin = {} AND departure_time = {};",
            airport.code, date
        ),
    };

    let mut protocol = Protocol::new();
    protocol.set_compress_algorithm(COMPRESSION);
    protocol.write_query(
        query.as_str(),
        consistency::Consistency::Quorum,
        vec![QueryFlags::SkipMetadata],
    )?;

    let message = conect_server(server, Some(protocol), &COMPRESSION)?;
    match message {
        ReplyMessage(msg) => match msg {
            ResponseMessage::Result { kind } => match kind {
                ResultResponse::Rows { metadata: _, rows } => Ok(rows),
                _ => Err(ErrorTypes::new(604, "Unexpected message".to_string())),
            },
            _ => Err(ErrorTypes::new(605, "Unexpected message".to_string())),
        },
        _ => Err(ErrorTypes::new(606, "Error receiving message".to_string())),
    }
}

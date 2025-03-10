use egui::{Align2, Color32, Painter, Response, RichText, TextEdit, Window};
use std::{collections::HashMap, fs, net::TcpStream};

use native_tls::TlsStream;
use walkers::{
    extras::{Place, Places, Style},
    Plugin, Position, Projector,
};

use crate::ui::windows::Airport;
use crate::{errors::error_types::ErrorTypes, ui::places};

use super::{
    flight::Flight,
    map::CustomMapMemory,
    windows::{is_valid_date, switch_flight_state, AppState},
};

fn read_airports() -> Vec<Airport> {
    match fs::read_to_string("airports.json") {
        Ok(data) => match serde_json::from_str(&data) {
            Ok(airports) => airports,
            Err(err) => {
                eprintln!("Failed to parse airports.json: {}", err);
                vec![]
            }
        },
        Err(err) => {
            eprintln!("Failed to read airports.json: {}", err);
            vec![]
        }
    }
}

/// Creates a built-in `Places` plugin with some predefined places by a `Position` instance from the longitude and latitude values of an airport.
///
/// The longitude and latitude values are parsed from the airport's string fields and
/// unwrapped to their respective floating-point representations.
///
/// # Panics
///
/// This function will panic if the parsing of the longitude or latitude values fails.
pub fn places() -> impl Plugin {
    let mut places: Vec<Place> = Vec::new();
    let airports: Vec<Airport> = read_airports();

    for airport in airports {
        let position =
            Position::from_lon_lat(airport.lon.parse().unwrap(), airport.lat.parse().unwrap());
        let label = airport.code;
        let symbol = '🏢';
        let style = Style::default();

        places.push(Place {
            position,
            label,
            symbol,
            style,
        });
    }

    Places::new(places)

    //Places::new(vec![])
}

/// Sample map plugin which draws custom stuff on the map.
pub struct CustomShapes {}

/// Implementation of the `Plugin` trait for the `CustomShapes` struct.
///
/// This implementation defines the behavior of the `run` method, which is responsible for
/// rendering custom shapes on the screen based on the position provided by the `places::facultad_de_ingenieria` function.
///
/// # Arguments
///
/// * `response` - A reference to a `Response` object, which contains information about user interactions.
/// * `_painter` - A `Painter` object used for drawing shapes on the screen (currently unused).
/// * `projector` - A reference to a `Projector` object, which is used to convert geographical coordinates to screen positions.
///
/// # Method
///
/// The `run` method performs the following steps:
///
/// 1. Retrieves the position of the point where the shapes should be placed using the `places::facultad_de_ingenieria` function.
/// 2. Projects this position to screen coordinates using the `projector` object.
/// 3. Defines a radius for the shapes.
/// 4. Checks if the mouse hover position is within the defined radius of the shape's position.
/// 5. (Commented out) Draws a filled circle at the projected position with a color that depends on whether the shape is hovered or not.
impl Plugin for CustomShapes {
    fn run(&mut self, response: &Response, _painter: Painter, projector: &Projector) {
        // Position of the point we want to put our shapes.
        let position = places::facultad_de_ingenieria();

        // Project it into the position on the screen.
        let position = projector.project(position).to_pos2();

        let radius = 30.;

        let _hovered = response
            .hover_pos()
            .map(|hover_pos| hover_pos.distance(position) < radius)
            .unwrap_or(false);

        // painter.circle_filled(
        //     position,
        //     radius,
        //     Color32::BLACK.gamma_multiply(if hovered { 0.5 } else { 0.2 }),
        // );
    }
}

#[derive(Default)]
pub struct ClickWatcher {
    pub clicked_at: Option<Position>,
    pub selected_airport: Option<Airport>,
    pub map_memory: CustomMapMemory,
    pub app_state: AppState,
    pub search_clicked: bool,
    pub actual_date: String,
    pub selected_plane: Option<(String, Vec<String>)>,
    pub planes: HashMap<(String, String), Flight>,
}

impl ClickWatcher {
    /// Creates a new `ClickWatcher` instance.
    ///
    /// # Returns
    /// A new `ClickWatcher` instance with loaded airports.
    ///
    /// # Panics
    /// This function will panic if loading airports from the JSON file fails.
    pub fn new(map_memory: CustomMapMemory) -> Self {
        Self {
            clicked_at: None,
            selected_airport: None,
            map_memory,
            app_state: AppState::default(),
            search_clicked: false,
            actual_date: String::new(),
            selected_plane: None,
            planes: HashMap::new(),
        }
    }

    /// Displays the position of the last click event in a UI window.
    ///
    /// # Arguments
    /// * `ui` - A reference to the `egui::Ui` instance where the position will be displayed.
    pub fn show_position(&mut self, ui: &egui::Ui) {
        if let Some(clicked_at) = self.clicked_at {
            egui::Window::new("Clicked Position")
                .collapsible(false)
                .resizable(false)
                .title_bar(false)
                .anchor(egui::Align2::CENTER_BOTTOM, [0., -10.])
                .show(ui.ctx(), |ui| {
                    ui.label(format!("{:.04} {:.04}", clicked_at.lon(), clicked_at.lat()))
                        .on_hover_text("last clicked position");
                });
        }
    }

    pub fn clicked_airport_info(
        &mut self,
        ui: &egui::Ui,
        server: &mut TlsStream<TcpStream>,
        elapsed_time: u64,
    ) -> Result<(), ErrorTypes> {
        if let Some(airport) = &self.selected_airport {
            Window::new("Airport Information")
                .collapsible(false)
                .resizable(false)
                .title_bar(false)
                .anchor(Align2::RIGHT_TOP, [-10., 10.])
                .show(ui.ctx(), |ui| {
                    ui.vertical(|ui| {
                        ui.push_id("search section", |ui| {
                            ui.label("Enter a date:");

                            // Input field for the date
                            ui.add(
                                TextEdit::singleline(&mut self.app_state.search_date)
                                    .hint_text("YYYY-MM-DD"), // Hint for the date format
                            );

                            if self.actual_date != self.app_state.search_date {
                                self.search_clicked = false;
                            }

                            // Search button
                            if ui.button("Search").clicked() {
                                self.search_clicked = true;
                            }

                            if self.search_clicked && is_valid_date(&self.app_state.search_date)? {
                                self.app_state.search_ui(ui, airport, server, elapsed_time);
                            }

                            self.actual_date = self.app_state.search_date.clone();
                            Ok::<(), ErrorTypes>(())
                        });
                    });
                });
        }
        Err(ErrorTypes::new(
            623,
            "Error getting airport information".to_string(),
        ))
    }

    pub fn clicked_plane_info(
        &mut self,
        ui: &egui::Ui,
        server: &mut TlsStream<TcpStream>,
        _elapsed_time: u64,
    ) {
        if let Some(plane) = &self.selected_plane {
            let mut actual_status = String::new();
            let plane_id = &plane.0;
            Window::new("Plane Information")
                .collapsible(false)
                .resizable(true)
                .title_bar(false)
                .anchor(Align2::RIGHT_TOP, [-10., 100.])
                .show(ui.ctx(), |ui| {
                    ui.vertical(|ui| {
                        ui.push_id("Plane info", |ui| {
                            ui.label(RichText::new("Plane information").size(18.0));
                            ui.separator();
                            let plane_data = match self.app_state.search_plane_info(
                                plane_id,
                                plane.1[1..].to_vec(),
                                server,
                            ) {
                                Ok(plane_data) => plane_data,
                                Err(err) => {
                                    eprintln!("Error getting plane data: {:?}", err);
                                    return;
                                }
                            };
                            let headers = plane_data[0].clone();
                            let data = plane_data[1].to_vec();
                            let result = (headers, data);

                            for (header, value) in result.0.iter().zip(result.1.iter()) {
                                if header == "status" {
                                    actual_status = value.clone();
                                }
                                ui.vertical(|ui| {
                                    ui.label(format!("{}: {}", header, value));
                                });
                            }
                        });
                    });
                });

            Window::new("Plane state")
                .collapsible(false)
                .resizable(false)
                .title_bar(false)
                .anchor(Align2::RIGHT_BOTTOM, [-10., 10.])
                .show(ui.ctx(), |ui| {
                    ui.vertical(|ui| {
                        ui.push_id("Plane state", |ui| {
                            ui.label("Plane state");
                            ui.separator();

                            // Add a selector for plane status
                            ui.label("Status:");
                            let statuses = ["ON TIME", "DELAYED"];
                            let mut selected_status =
                                if actual_status == "ON TIME" { 0 } else { 1 };

                            egui::ComboBox::from_label("Select status")
                                .selected_text(statuses[selected_status])
                                .show_ui(ui, |ui| {
                                    for (index, status) in statuses.iter().enumerate() {
                                        if ui
                                            .selectable_value(&mut selected_status, index, *status)
                                            .clicked()
                                            && actual_status != *status
                                        {
                                            let _ = switch_flight_state(
                                                server,
                                                plane_id.clone(),
                                                status.to_string(),
                                                plane.1[1..].to_vec(),
                                            );
                                        }
                                    }
                                });
                        });
                    });
                });
        }
    }

    /// Gets the currently selected airport, if any.
    ///
    /// # Returns
    /// An `Option` containing a reference to the selected `Airport`, or `None` if no airport is selected.
    pub fn get_selected_airport(&self) -> Option<&Airport> {
        self.selected_airport.as_ref()
    }
}

/// Implementation of the `Plugin` trait for `ClickWatcher`.
///
/// This implementation handles the click events on the UI and projects the click position
/// to the world coordinates. It checks if the click is within a certain distance of any
/// airport and selects the airport if it is within the threshold. It also draws a filled
/// circle at the clicked position.
///
/// # Methods
///
/// - `run(&mut self, response: &Response, painter: Painter, projector: &Projector)`:
///   Handles the click events and projects the click position to the world coordinates.
///   It selects an airport if the click is within a certain distance of any airport and
///   draws a filled circle at the clicked position.
///
/// # Parameters
///
/// - `response`: The response from the UI interaction.
/// - `painter`: The painter used to draw on the UI.
/// - `projector`: The projector used to convert screen coordinates to world coordinates.
///
/// # Behavior
///
/// - If the response has not changed and the primary button is clicked:
///   - It calculates the world position of the click.
///   - It iterates through the list of airports and checks if the click is within 100.0 units
///     of any airport. If so, it selects the airport.
///   - It stores the world position of the click.
/// - If there is a stored click position, it draws a filled circle at the projected position
///   on the screen.
impl Plugin for &mut ClickWatcher {
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector) {
        if !response.changed() && response.clicked_by(egui::PointerButton::Primary) {
            if let Some(pointer_pos) = response.interact_pointer_pos() {
                let world_pos = projector.unproject(pointer_pos - response.rect.center());
                for plane in self.app_state.planes_positions.iter() {
                    let plane_screen_pos = projector
                        .project(Position::from_lon_lat(
                            plane.1[5].parse().unwrap(),
                            plane.1[4].parse().unwrap(),
                        ))
                        .to_pos2();
                    let distance = plane_screen_pos.distance(pointer_pos);
                    if distance < 50.0 {
                        let plane_id = plane.0 .0.clone();
                        let mut plane_info: Vec<String> = vec![plane.0 .1.clone()];
                        plane_info.extend(plane.1.clone());

                        self.selected_plane = Some((plane_id, plane_info));

                        break;
                    } else {
                        self.selected_plane = None;
                    }
                }
                self.clicked_at = Some(world_pos);
            }
        }

        if !response.changed() && response.clicked_by(egui::PointerButton::Primary) {
            if let Some(pointer_pos) = response.interact_pointer_pos() {
                let world_pos = projector.unproject(pointer_pos - response.rect.center());
                for airport in &self.map_memory.airports_postions {
                    let airport_screen_pos = projector
                        .project(Position::from_lon_lat(
                            airport.1.lon.parse().unwrap(),
                            airport.1.lat.parse().unwrap(),
                        ))
                        .to_pos2();
                    let distance = airport_screen_pos.distance(pointer_pos);

                    if distance < 100.0 {
                        self.selected_airport = Some(airport.1.clone());
                        break;
                    } else if self.selected_plane.is_none() {
                        self.selected_airport = None;
                    }
                }
                self.clicked_at = Some(world_pos);
            }
        }

        if let Some(position) = self.clicked_at {
            painter.circle_filled(projector.project(position).to_pos2(), 5.0, Color32::BLUE);
        }

        if self.selected_airport.is_some() {
            if self.app_state.search_results.is_none() {
                return;
            }

            if self
                .app_state
                .search_results
                .clone()
                .unwrap()
                .arrivals
                .len()
                == 1
                && self
                    .app_state
                    .search_results
                    .clone()
                    .unwrap()
                    .departures
                    .len()
                    == 1
            {
                return;
            }
            let mut arrivals = Vec::new();
            if let Some(results) = self.app_state.search_results.clone() {
                arrivals = results.arrivals.clone();
            }
            let mut departures = Vec::new();
            if let Some(results) = self.app_state.search_results.clone() {
                departures = results.departures.clone();
            }

            let vec = [arrivals, departures];
            let colors = [Color32::RED, Color32::GREEN];

            for (i, result) in vec.iter().enumerate() {
                if result.len() <= 1 {
                    continue;
                }
                for plane_directions in &result[1..] {
                    let origin = self
                        .map_memory
                        .airport_locations_by_name
                        .get(&plane_directions[2]);
                    let destination = self
                        .map_memory
                        .airport_locations_by_name
                        .get(&plane_directions[3]);

                    if let (Some(origin), Some(destination)) = (origin, destination) {
                        let origin_position;
                        let stroke = egui::Stroke::new(3.0, colors[i]);
                        if let (Ok(lon), Ok(lat)) = (origin.1.parse(), origin.0.parse()) {
                            origin_position = projector
                                .project(Position::from_lon_lat(lon, lat))
                                .to_pos2();
                        } else {
                            continue;
                        }
                        let destination_position;
                        if let (Ok(lon), Ok(lat)) = (destination.1.parse(), destination.0.parse()) {
                            destination_position = projector
                                .project(Position::from_lon_lat(lon, lat))
                                .to_pos2();
                        } else {
                            continue;
                        }
                        painter.line_segment([origin_position, destination_position], stroke);
                    }
                }
            }
        }

        if !self.app_state.planes_positions.is_empty() && self.selected_airport.is_some() {
            for plane in self.app_state.planes_positions.iter() {
                if let (Ok(lon), Ok(lat)) = (plane.1[5].parse(), plane.1[4].parse()) {
                    let position = Position::from_lon_lat(lon, lat);
                    let screen_position = projector.project(position).to_pos2();
                    let radius = 5.0;
                    let color = Color32::BLACK;
                    painter.circle_filled(screen_position, radius, color);
                }
            }
        }
    }
}

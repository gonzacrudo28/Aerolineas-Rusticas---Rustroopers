use crate::ui::windows::Airport;
use std::{collections::HashMap, fs};
use walkers::MapMemory;
#[derive(Debug, Default)]
pub struct CustomMapMemory {
    pub map_memory: MapMemory,
    pub airports_postions: HashMap<(String, String), Airport>,
    pub airport_locations_by_name: HashMap<String, (String, String)>,
}

impl CustomMapMemory {
    pub fn new() -> Self {
        let (airports_postions, airport_locations_by_name) = match Self::load_airports() {
            Ok((positions, locations)) => (positions, locations),
            Err(e) => {
                eprintln!("Failed to load airports: {}", e);
                (HashMap::new(), HashMap::new())
            }
        };
        println!("Loaded {} airports", airports_postions.len());
        Self {
            map_memory: MapMemory::default(),
            airports_postions,
            airport_locations_by_name,
        }
    }

    #[allow(clippy::type_complexity)]
    fn load_airports() -> Result<
        (
            HashMap<(String, String), Airport>,
            HashMap<String, (String, String)>,
        ),
        Box<dyn std::error::Error>,
    > {
        let data = fs::read_to_string("airports.json")?;
        let airport_list: Vec<Airport> = serde_json::from_str(&data)?;

        let mut airports = HashMap::new();
        let mut airports_by_name = HashMap::new();
        for airport in airport_list {
            airports.insert((airport.lat.clone(), airport.lon.clone()), airport.clone());
            airports_by_name.insert(
                airport.code.clone(),
                (airport.lat.clone(), airport.lon.clone()),
            );
        }

        Ok((airports, airports_by_name))
    }
}

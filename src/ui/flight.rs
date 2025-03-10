use crate::{errors::error_types::ErrorTypes, server::query_execute::min_fuel};

use super::windows::Airport;

const FRACCION_ADVANCE: f64 = 0.1;

#[derive(Debug, Clone)]
pub struct Flight {
    pub flight_code: i32,
    pub origin: Airport,
    pub destination: Airport,
    pub departure_time: String,
    pub arrival_time: String,
    pub latitude: f64,
    pub longitude: f64,
    pub height: f64,
    pub velocity: f64,
    pub distance: f64,
    pub fuel: f64,
    pub distance_traveled: f64,
}

impl Flight {
    pub fn new(
        flight_code: i32,
        origin: Airport,
        destination: Airport,
        departure_time: String,
        arrival_time: String,
        distance: f64,
        fuel: f64,
    ) -> Self {
        let mut latitude = 0.0;
        if let Ok(latitude_) = origin.get_latitude() {
            latitude = latitude_
        };
        let mut longitude = 0.0;
        if let Ok(longitude_) = origin.get_longitude() {
            longitude = longitude_
        };
        Flight {
            flight_code,
            origin: origin.clone(),
            destination,
            departure_time,
            arrival_time,
            velocity: 0.0,
            latitude,
            longitude,
            height: 0.0,
            distance_traveled: 0.0,
            distance,
            fuel,
        }
    }

    pub fn get_flight_code(&self) -> i32 {
        self.flight_code
    }

    pub fn get_origin(&self) -> &Airport {
        &self.origin
    }

    pub fn get_destination(&self) -> &Airport {
        &self.destination
    }

    pub fn get_departure_time(&self) -> &str {
        &self.departure_time
    }

    pub fn get_distance_traveled(&self) -> f64 {
        self.distance_traveled
    }

    pub fn get_arrival_time(&self) -> &str {
        &self.arrival_time
    }

    pub fn get_fuel(&self) -> f64 {
        self.fuel
    }

    pub fn get_latitude(&self) -> f64 {
        self.latitude
    }

    pub fn set_latitude(&mut self, latitude: f64) {
        self.latitude = latitude;
    }

    pub fn get_longitude(&self) -> f64 {
        self.longitude
    }

    pub fn set_longitude(&mut self, longitude: f64) {
        self.longitude = longitude;
    }

    pub fn get_velocity(&self) -> f64 {
        self.velocity
    }

    pub fn set_velocity(&mut self, velocity: f64) {
        self.velocity = velocity;
    }

    pub fn get_height(&self) -> f64 {
        self.height
    }

    pub fn set_distance(&mut self, distance: f64) {
        self.distance = distance;
    }

    pub fn update_flight(&mut self) -> Result<(), ErrorTypes> {
        self.distance_traveled += self.distance * FRACCION_ADVANCE;
        self.distance -= self.distance * FRACCION_ADVANCE;
        if self.distance_traveled >= self.distance {
            self.distance_traveled = self.distance;
            self.height = 0.0;
            self.velocity = 0.0;
            self.latitude = self.destination.get_latitude()?;
            self.longitude = self.destination.get_longitude()?;
        } else {
            self.fuel -= min_fuel(self.distance) * FRACCION_ADVANCE;
            self.update_position()?;
            let progress = self.distance_traveled / self.distance;
            if !(0.1..=0.9).contains(&progress) {
                self.height = 1000.0;
                self.velocity = 700.0;
            } else if !(0.3..=0.8).contains(&progress) {
                self.height = 8000.0;
                self.velocity = 810.0;
            } else {
                self.height = 10000.0;
                self.velocity = 950.0;
            }
        }
        Ok(())
    }

    pub fn update_position(&mut self) -> Result<(), ErrorTypes> {
        let lat_diff = (self.destination.get_latitude()? - self.origin.get_latitude()?).abs();
        let long_diff = (self.destination.get_longitude()? - self.origin.get_longitude()?).abs();
        if self.destination.get_latitude()? > self.origin.get_latitude()? {
            self.latitude += lat_diff * FRACCION_ADVANCE;
        } else {
            self.latitude -= lat_diff * FRACCION_ADVANCE;
        }
        if self.destination.get_longitude()? > self.origin.get_longitude()? {
            self.longitude += long_diff * FRACCION_ADVANCE;
        } else {
            self.longitude -= long_diff * FRACCION_ADVANCE;
        }

        Ok(())
    }
    pub fn get_distance(&mut self) -> f64 {
        self.distance - self.distance_traveled
    }
}

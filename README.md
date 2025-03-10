# Aerolineas Rusticas
[![language](https://img.shields.io/badge/language-Rust-green.svg?style=flat-square)](https://www.rust-lang.org/es)
![os](https://img.shields.io/badge/OS-linux-blue.svg?style=flat-square)

<p align="center">
  <img src="logo.png" alt="Aerolines rusticas logo" width="500">
</p>

Programa de seguimiento de vuelos desarrollado como trabajo práctico para Taller de Programación (TA045) en FIUBA.

**Integrantes:**

- [Facundo Calderan](https://github.com/fcalderan19)
- [Gonzalo Crudo](https://github.com/gonzacrudo28)
- [Mariano Merlinsky](https://github.com/Mario-Merlinsky)
- [Nicolás Chaia](https://github.com/NicolasChaia)

## ⚡️ Dependencias
Las dependecias necesarias para instalar y correr el programa son:

Nodos:
- murmur3 (0.5.2)
- rand (0.8.5)
- chksum-md5 (0.0.0)
- chrono (0.4.38)

Simulador:
- threadpool (1.8.1)

Compresión:
- snap (1.1)
- lz4 (1.24)

Interfaz Grafica:
- egui (0.29.1)
- walkers (0.25.0)
- env_logger (0.11)
- eframe (0.29.1)
- serde_json (1.0.128)
- egui_extras (0.29.1)

Encriptacion:
- native-tls (0.2.12)

## 📦 Instalacion:
Para la ejecucion de los nodos se debera instalar x-term.
Su utilidad es la de crear una terminal propia para cada nodo.
```bash
sudo apt-get install xterm
```

Para la generacion de certificados se debera instalar open ssl.
```bash
sudo apt install openssl
```

## 🚀 Ejecución:
### Nodos:
Para levantar los nodos se debera ejecutar el siguiente comando.
```bash
./scripts/launch_nodes.sh
```
Para levantar un solo nodo en particular se debera ejecutar el siguiente comando.
```bash
cargo run --bin node -- <INTERNAL_IP_ADDRESS> <CLIENT_IP_ADDRESS>
```
Por el contrario para cerrarlos se hara con el comando.
```bash
./scripts/kill_nodes.sh
```

### Nodos en containers:
Primero debemos buildear la imagen.
```bash
docker build -t node .
```
Para levantar los nodos se debera ejecutar los siguientes comandos.
```bash
docker-compose up

./scripts/launch_nodes_docker.sh
```
Para levantar un solo nodo en particular se debera ejecutar el siguiente comando.
```bash
./scripts/launch_node_docker.sh <INTERNAL_PORT> <EXTERNAL_PORT>
```
Ejemplo de uso:
```bash
./scripts/launch_node_docker.sh 8088 8098
```
Por el contrario para cerrarlos se hara con el comando.
```bash
docker-compose down
```
Para cerrar un solo nodo en particular se debera ejecutar el siguiente comando.
```bash
docker stop node-<INTERNAL_PORT>
docker rm node-<INTERNAL_PORT>
```
Ejemplo de uso:
```bash
docker stop node-8080
docker rm node-8080
```


### Interfaz Grafica:
Para levantar la conexion con el cliente ui se debera correr el comando.
```bash
cargo run --bin main
```

### Simulador:
Para levantar la conexion del servido con el simulador se debera correr el comando.
```bash
cargo run --bin simulador
```

Ademas se debera autenticar el usario utilizando.

***Usuario: simulator***

***Contraseña: 0000***


### Comandos para ver logs
Para entrar al container
```bash
docker exec -it <nombre-del-container> /bin/bash 
```
Luego
```bash
cat node<puerto-del-nodo>_log.log
```

### Comandos auxiliares para docker

```bash
docker images #Para listar todas las imagenes

docker ps -a #Para listar todos los contenedores (incluyendo los detenidos)

docker exec -it <container_name> /bin/bash #Inicializa una terminal interactiva dentro del contenedor especificado
```

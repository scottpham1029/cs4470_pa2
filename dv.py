# Implementation overview:
# Each process reads its own topology file to identify its local server, all
# server addresses, and direct link costs, then binds a UDP socket locally.
# The initial routing table contains cost 0 to itself, configured costs to
# neighbors, and infinity to all other destinations.
# A receiver thread validates neighbor updates and saves their distance vectors.
# Bellman-Ford recomputes routes after updates, link changes, and timeouts.
# A periodic thread checks for neighbors silent for three update intervals and
# advertises the current table. Only this thread and the step command send updates.
# The main thread handles update/disable (local link changes), display (sorted
# routes), packets (read and reset the receive count), and crash (stop the server).
# A shared lock protects routing state and the packet count across these threads;
# a stop event tells the worker threads to finish before the socket is closed.

import argparse
import socket
import threading
import struct
import time

packet_count = 0

def read_topology(filename):
    # Each topology file lists all routers, but only local direct links.
    with open(filename, "r") as file:
        lines = [line.strip() for line in file if line.strip()]

    if len(lines) < 2:
        raise ValueError("Incomplete topology file")
    num_servers = int(lines[0])
    num_neighbors = int(lines[1])

    if num_servers < 1 or num_neighbors < 0:
        raise ValueError("Invalid server or neighbor count")
    
    if len(lines) != 2 + num_servers + num_neighbors:
        raise ValueError("Topology line count does not match header")

    servers = {}
    for line in lines[2:2 + num_servers]:
        server_id, ip, port = line.split()
        server_id, port = int(server_id), int(port)

        if server_id in servers:
            raise ValueError("Duplicate server ID")
        
        if not (1 <= port <= 65535):
            raise ValueError("Invalid server port")
        
        servers[server_id] = {"ip": ip, "port": port}

    links = [tuple(map(int, line.split()))
             for line in lines[2 + num_servers:]]

    # All direct links share the local server ID (for this assignment's topology).
    possible_ids = set(servers)
    for source, destination, cost in links:
        if source not in servers or destination not in servers or source == destination:
            raise ValueError("Invalid link endpoints")
        
        if not (0 <= cost < INFINITY):
            raise ValueError("Invalid initial link cost")
        
        possible_ids &= {source, destination}

    if len(possible_ids) != 1:
        raise ValueError("Cannot uniquely determine local server ID from links")
    
    my_id = possible_ids.pop()

    neighbors = {}
    for source, destination, cost in links:
        neighbor_id = destination if source == my_id else source

        if neighbor_id in neighbors:
            raise ValueError("Duplicate neighbor link")
        
        neighbors[neighbor_id] = cost

    return my_id, num_servers, num_neighbors, servers, neighbors


#UDP connnection
def initialize_socket(my_id, servers):
    my_ip = servers[my_id]["ip"]
    my_port = servers[my_id]["port"]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((my_ip, my_port))

    print(f"Server {my_id} listening on {my_ip}:{my_port}")

    return sock


def send_messages(sock, my_id, servers, neighbors, routing_table):

    # Create the encoded distance-vector packet
    packet = create_routing_packet(
        my_id, servers, routing_table
    )

    for neighbor_id, cost in neighbors.items():

        # Do not send through a disabled link
        if cost == float("inf"):
            continue

        neighbor_ip = servers[neighbor_id]["ip"]
        neighbor_port = servers[neighbor_id]["port"]

        sock.sendto(
            packet,
            (neighbor_ip, neighbor_port)
        )

        print(f"Sent routing update to Server {neighbor_id}")

    #verify number of bytes sent
    #print(f"Packet size: {len(packet)} bytes")


def receive_messages(sock, my_id, servers, neighbors,
                     neighbor_vectors, routing_table, routing_lock,
                     last_seen, configured_costs, timed_out, stop_event):
    global packet_count

    while not stop_event.is_set():
        try:
            data, address = sock.recvfrom(4096)
            sender_ip, sender_port, distance_vector = decode_routing_packet(data)

            sender_id = None
            for server_id, info in servers.items():
                if info["ip"] == sender_ip and info["port"] == sender_port:
                    sender_id = server_id
                    break

            if sender_id not in neighbors or address != (sender_ip, sender_port):
                continue

            if set(distance_vector) != set(servers):
                continue

            with routing_lock:
                # Explicitly disabled links stay down. Timeouts can recover.
                if neighbors[sender_id] == float("inf"):
                    if sender_id not in timed_out:
                        continue

                    neighbors[sender_id] = configured_costs[sender_id]
                    timed_out.remove(sender_id)
                    print(f"\nServer {sender_id} is available again.")

                last_seen[sender_id] = time.monotonic()
                packet_count += 1

                print(f"\nRECEIVED A MESSAGE FROM SERVER {sender_id}")

                neighbor_vectors[sender_id] = distance_vector
                if bellman_ford(my_id, servers, neighbors,
                                neighbor_vectors, routing_table):
                    print("Routing table updated.")

        except socket.timeout:
            continue

        except ValueError as error:
            print(f"\nInvalid routing packet: {error}")

        except OSError:
            if not stop_event.is_set():
                print("\nUDP receiver stopped due to socket error.")
            break




def initialize_routing_table(my_id, servers, neighbors):
    # ROUTING TABLE DATA STRUCTURE:
    # routing_table[destination_server_id] = {"next_hop": server_id, "cost": cost}
    # Each destination maps to its next hop and total path cost. The local route
    # points to itself with cost 0. Unreachable routes use next_hop=None and
    # cost=float("inf"); display renders these as -1 and inf, respectively.
    routing_table = {}

    for server_id in servers:

        #my server
        if server_id == my_id:
            routing_table[server_id] = {
                "next_hop": my_id,
                "cost": 0
            }

        #neighbor
        elif server_id in neighbors:
            routing_table[server_id] = {
                "next_hop": server_id,
                "cost": neighbors[server_id]
            }

        #not neighbor
        else:
            routing_table[server_id] = {
                "next_hop": None,
                "cost": float("inf")
            }

    return routing_table


# Maximum 16-bit unsigned value represents infinity
INFINITY = 65535

# ROUTING UPDATE MESSAGE DATA STRUCTURE (one UDP datagram):
# Header: 2-byte count, 2-byte sender port, 4-byte sender IP
# Entry: 4-byte IP, 2-byte port, 2-byte reserved,
#        2-byte ID, 2-byte cost
# All integers use network byte order (big-endian); the reserved field is zero.
# The header is followed by one entry per destination, including the sender.
# Total size is 8 + 12 * count bytes. Cost 65535 encodes infinity on the wire;
# decoding converts it back to float("inf") for routing calculations.

def create_routing_packet(my_id, servers, routing_table):
    my_ip = servers[my_id]["ip"]
    my_port = servers[my_id]["port"]
    
    packet = struct.pack("!HH4s", len(routing_table), my_port,
                         socket.inet_aton(my_ip))

    # Packet entry: IPv4 (4), port (2), reserved (2), ID (2), cost (2).
    for server_id in sorted(routing_table):
        server_ip = servers[server_id]["ip"]
        server_port = servers[server_id]["port"]
        cost = routing_table[server_id]["cost"]
        wire_cost = INFINITY if cost >= INFINITY else int(cost)
        packet += struct.pack("!4sHHHH", socket.inet_aton(server_ip),
                              server_port, 0, server_id, wire_cost)
    return packet



def decode_routing_packet(data):

    # Header is 8 bytes
    if len(data) < 8:
        raise ValueError("Packet is too short")

    num_entries, sender_port, sender_ip_bytes = struct.unpack(
        "!HH4s", data[:8]
    )

    sender_ip = socket.inet_ntoa(sender_ip_bytes)

    # Every routing entry is 12 bytes
    expected_size = 8 + (num_entries * 12)

    if len(data) != expected_size:
        raise ValueError("Invalid routing packet size")

    distance_vector = {}

    offset = 8

    for _ in range(num_entries):

        entry = data[offset:offset + 12]

        ip_bytes, port, reserved, server_id, cost = struct.unpack(
            "!4sHHHH", entry
        )

        if reserved != 0:
            raise ValueError("Invalid reserved field")

        if server_id in distance_vector:
            raise ValueError("Duplicate destination ID")

        distance_vector[server_id] = (
            float("inf") if cost == INFINITY else cost
        )

        offset += 12

    return sender_ip, sender_port, distance_vector


# Display destinations in ascending ID order using the required three columns.
def display_routing_table(routing_table):
    # Exact three-column format required by the assignment.
    # An unreachable destination has no next hop (-1) and infinite cost.
    for destination in sorted(routing_table):
        next_hop = routing_table[destination]["next_hop"]
        cost = routing_table[destination]["cost"]
        print(f"{destination} {next_hop if next_hop is not None else -1} "
              f"{'inf' if cost == float('inf') else cost}")



def bellman_ford(my_id, servers, neighbors,
                 neighbor_vectors, routing_table):
    # For each destination d, compare the direct link with every usable route
    # through neighbor n: link_cost(n) + neighbor_vectors[n][d]. Keep the lowest
    # cost and its next hop; keep the local server's own cost fixed at zero.
    updated = False

    for destination in servers:
        if destination == my_id:
            best_cost, best_next_hop = 0, my_id
        else:
            best_cost = neighbors.get(destination, float("inf"))
            best_next_hop = destination if best_cost != float("inf") else None

            for neighbor_id, link_cost in neighbors.items():
                if link_cost == float("inf") or neighbor_id not in neighbor_vectors:
                    continue

                neighbor_cost = neighbor_vectors[neighbor_id].get(
                    destination, float("inf"))
                
                candidate = link_cost + neighbor_cost
                # Reserve the 16-bit 65535 value for infinity.
                
                if candidate >= INFINITY:
                    candidate = float("inf")

                if candidate < best_cost:
                    best_cost, best_next_hop = candidate, neighbor_id

        old_route = routing_table[destination]

        if old_route["cost"] != best_cost or old_route["next_hop"] != best_next_hop:
            routing_table[destination] = {
                "next_hop": best_next_hop, "cost": best_cost
            }
            updated = True
    
    return updated


def update_link(my_id, server1, server2, new_cost, servers,
                neighbors, neighbor_vectors, routing_table):
    
    if server1 not in servers or server2 not in servers or server1 == server2:
        raise ValueError("Invalid server IDs")
    
    if my_id not in (server1, server2):
        raise ValueError("Link does not involve this server")
    
    neighbor_id = server2 if my_id == server1 else server1

    if neighbor_id not in neighbors:
        raise ValueError("Server is not a direct neighbor")
    
    if new_cost != float("inf") and not (0 <= new_cost < INFINITY):
        raise ValueError("Invalid link cost")

    neighbors[neighbor_id] = new_cost
    # Reuse the neighbor's latest vector with the new finite link cost.
    # Discard it only when the link is disabled.
    if new_cost == float("inf"):
        neighbor_vectors.pop(neighbor_id, None)
    return bellman_ford(my_id, servers, neighbors,
                        neighbor_vectors, routing_table)


def commands():
    print(
        "\nType 'step' to send a routing update.\n"
        "Type 'update <server-ID1> <server-ID2> <New Link Cost>' to change a link.\n"
        "Type 'packets' to display and reset the received packet count.\n"
        "Type 'display' to display the routing table.\n"
        "Type 'disable <server-ID>' to disable a direct link.\n"
        "Type 'crash' to simulate a server crash.\n"
        "Type 'exit' to stop the server.\n"
        "Type 'commands' to show this list.\n"
    )


def periodic_updates(sock, my_id, servers, neighbors,
                     neighbor_vectors, routing_table,
                     routing_lock, last_seen, configured_costs,
                     timed_out, interval, stop_event):
    
    # Check silence at each periodic tick. After at least three intervals without
    # an accepted update, invalidate the link and cached vector, then recompute.
    # Keep the destination in the routing table; another neighbor may reach it.
    while not stop_event.wait(interval):
        with routing_lock:
            now = time.monotonic()
            for neighbor_id, link_cost in list(neighbors.items()):
                if (link_cost != float("inf")
                        and now - last_seen[neighbor_id] >= 3 * interval):
                    neighbors[neighbor_id] = float("inf")
                    neighbor_vectors.pop(neighbor_id, None)
                    timed_out.add(neighbor_id)

                    print(f"\nServer {neighbor_id} timed out.")
                    bellman_ford(my_id, servers, neighbors,
                                 neighbor_vectors, routing_table)

            # No triggered advertisements: only periodic or step sends.
            try:
                send_messages(sock, my_id, servers, neighbors, routing_table)
            except OSError as error:
                if not stop_event.is_set():
                    print(f"Periodic update ERROR: {error}")


def main():
    global packet_count

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", required=True, help="Topology file")
    parser.add_argument("-i", required=True, type=int, help="Update interval")
    
    args = parser.parse_args()
    
    if args.i <= 0:
        parser.error("Update interval must be positive")

    my_id, num_servers, num_neighbors, servers, neighbors = read_topology(args.t)
    
    routing_table = initialize_routing_table(my_id, servers, neighbors)
    # neighbor_vectors[neighbor_id][destination_id] stores the neighbor's most
    # recently advertised cost, used alongside our direct costs by Bellman-Ford.
    neighbor_vectors = {}

    configured_costs = neighbors.copy()  # last explicitly set link cost
    timed_out = set()                     # neighbors lost due to missed updates
    last_seen = {neighbor_id: time.monotonic() for neighbor_id in neighbors}
    routing_lock = threading.Lock()
    stop_event = threading.Event()

    sock = initialize_socket(my_id, servers)
    sock.settimeout(0.5)  # receiver can periodically check stop_event

    receiver_thread = threading.Thread(
        target=receive_messages,
        args=(sock, my_id, servers, neighbors, neighbor_vectors,
              routing_table, routing_lock, last_seen,
              configured_costs, timed_out, stop_event),
        daemon=True
    )
    
    receiver_thread.start()

    periodic_thread = threading.Thread(
        target=periodic_updates,
        args=(sock, my_id, servers, neighbors, neighbor_vectors,
              routing_table, routing_lock, last_seen,
              configured_costs, timed_out, args.i, stop_event),
        daemon=True
    )
    
    periodic_thread.start()
    commands()

    try:
        while True:
            command = input("> ").strip()
            command_parts = command.lower().split()
            if not command_parts:
                continue
            action = command_parts[0]

            if action == "step":
                if len(command_parts) != 1:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                try:
                    with routing_lock:
                        send_messages(sock, my_id, servers, neighbors, routing_table)
                    print(f"{command} SUCCESS")

                except (OSError, ValueError) as error:
                    print(f"{command} ERROR: {error}")

            elif action == "update":
                if len(command_parts) != 4:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                try:
                    server1, server2 = int(command_parts[1]), int(command_parts[2])
                    new_cost = (float("inf") if command_parts[3] == "inf"
                                else int(command_parts[3]))
                    
                    if new_cost != float("inf") and not (0 <= new_cost < INFINITY):
                        raise ValueError("Invalid link cost")
                    with routing_lock:
                        update_link(my_id, server1, server2, new_cost, servers,
                                    neighbors, neighbor_vectors, routing_table)
                        neighbor_id = server2 if my_id == server1 else server1
                        configured_costs[neighbor_id] = new_cost
                        timed_out.discard(neighbor_id)
                        last_seen[neighbor_id] = time.monotonic()
                    print(f"{command} SUCCESS")
                except ValueError as error:
                    print(f"{command} ERROR: {error}")

            elif action == "packets":
                if len(command_parts) != 1:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                with routing_lock:
                    print(f"Packets: {packet_count}")
                    packet_count = 0
                print(f"{command} SUCCESS")

            elif action == "disable":
                if len(command_parts) != 2:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                try:
                    neighbor_id = int(command_parts[1])
                    with routing_lock:
                        update_link(my_id, my_id, neighbor_id, float("inf"),
                                    servers, neighbors, neighbor_vectors,
                                    routing_table)
                        configured_costs[neighbor_id] = float("inf")
                        timed_out.discard(neighbor_id)
                    print(f"{command} SUCCESS")
                
                except ValueError as error:
                    print(f"{command} ERROR: {error}")

            elif action == "display":
                if len(command_parts) != 1:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                with routing_lock:
                    display_routing_table(routing_table)
                print(f"{command} SUCCESS")

            elif action == "crash":
                if len(command_parts) != 1:
                    print(f"{command} ERROR: Invalid arguments")
                    continue

                print(f"{command} SUCCESS")
                break

            elif action == "exit":
                break

            elif action == "commands":
                commands()
                
            else:
                print(f"{command} ERROR: Unknown command")

    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        stop_event.set()
        periodic_thread.join(timeout=2)
        receiver_thread.join(timeout=1)
        sock.close()
        print(f"Server {my_id} stopped.")

if __name__ == "__main__":
    main()

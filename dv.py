
import argparse
import socket
import threading
import struct


def read_topology(filename):
    #open and read topology file
    #store each lines in a list
    with open(filename, "r") as file:
        lines = [line.strip() for line in file if line.strip()]

    #first two lines
    num_servers = int(lines[0])
    num_neighbors = int(lines[1])

    #read all server IDs, IPs, and ports
    servers = {}

    for line in lines[2:2 + num_servers]:
        server_id, ip, port = line.split()

        servers[int(server_id)] = {
            "ip": ip,
            "port": int(port)
        }

    #read direct neighbor information
    neighbors = {}

    for line in lines[2 + num_servers:]:
        source, destination, cost = map(int, line.split())

        #figure 1 example: 1->2, cost is 7
        neighbors[destination] = cost

    #determine which server this topology file belongs to
    links = [
        tuple(map(int, line.split()))
        for line in lines[2 + num_servers:]
    ]

    possible_ids = set(servers)

    for source, destination, cost in links:
        possible_ids &= {source, destination}

    if len(possible_ids) != 1:
        raise ValueError("Cannot uniquely determine local server ID")

    my_id = possible_ids.pop()

    # Verify the number of neighbors
    if len(links) != num_neighbors:
        raise ValueError("Neighbor count does not match topology file")

    return my_id, num_servers, num_neighbors, servers, neighbors

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
                     neighbor_vectors, routing_table,
                     routing_lock):

    while True:
        try:
            data, address = sock.recvfrom(4096)

            sender_ip, sender_port, distance_vector = (
                decode_routing_packet(data)
            )

            # Identify the sending server
            sender_id = None

            for server_id, info in servers.items():
                if (
                    info["ip"] == sender_ip
                    and info["port"] == sender_port
                ):
                    sender_id = server_id
                    break

            # Only accept updates from direct neighbors
            if sender_id not in neighbors:
                continue

            # Verify the UDP source address
            if address != (sender_ip, sender_port):
                continue

            # Verify the destinations in the packet
            if set(distance_vector) != set(servers):
                continue

            with routing_lock:

                # Ignore disabled neighbors
                if neighbors[sender_id] == float("inf"):
                    continue

                print(
                    f"\nRECEIVED A MESSAGE FROM SERVER {sender_id}"
                )

                # Store the neighbor's latest distance vector
                neighbor_vectors[sender_id] = distance_vector

                # Recalculate our routing table
                updated = bellman_ford(
                    my_id,
                    servers,
                    neighbors,
                    neighbor_vectors,
                    routing_table
                )

                if updated:
                    print("Routing table updated.")

        except ValueError as error:
            print(f"\nInvalid routing packet: {error}")

        except OSError:
            break



def initialize_routing_table(my_id, servers, neighbors):
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

# Packet structure:
# Header: 2-byte count, 2-byte sender port, 4-byte sender IP
# Entry: 4-byte IP, 2-byte port, 2-byte reserved,
#        2-byte ID, 2-byte cost

def create_routing_packet(my_id, servers, routing_table):
    my_ip = servers[my_id]["ip"]
    my_port = servers[my_id]["port"]

    num_entries = len(routing_table)

    # Build packet header
    packet = struct.pack(
        "!HH4s",
        num_entries,
        my_port,
        socket.inet_aton(my_ip)
    )

    # Add each destination's routing information
    for server_id in sorted(routing_table):
        server_ip = servers[server_id]["ip"]
        server_port = servers[server_id]["port"]
        cost = routing_table[server_id]["cost"]

        if cost == float("inf"):
            cost = INFINITY

        entry = struct.pack(
            "!4sHHHH",
            socket.inet_aton(server_ip),
            server_port,
            0,
            server_id,
            cost
        )

        packet += entry

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


#temporary routing table format, not final explicit formal required by this assignment
def display_routing_table(routing_table):
    print("Destination\tNext Hop\tCost")

    for destination in sorted(routing_table):
        next_hop = routing_table[destination]["next_hop"]
        cost = routing_table[destination]["cost"]

        if next_hop is None:
            next_hop = "N/A"

        print(f"{destination}\t\t{next_hop}\t\t{cost}")


def bellman_ford(my_id, servers, neighbors,
                 neighbor_vectors, routing_table):

    updated = False

    # Calculate the best path to every destination
    for destination in servers:

        # The cost to reach ourselves is always zero
        if destination == my_id:
            best_cost = 0
            best_next_hop = my_id

        else:
            # Start with the direct link, if one exists
            best_cost = neighbors.get(
                destination, float("inf")
            )

            best_next_hop = (
                destination
                if best_cost != float("inf")
                else None
            )

            # Examine every directly connected neighbor
            for neighbor_id, link_cost in neighbors.items():

                # Ignore disabled or unreachable neighbors
                if link_cost == float("inf"):
                    continue

                # Skip neighbors that haven't sent a vector
                if neighbor_id not in neighbor_vectors:
                    continue

                # Neighbor's advertised cost to destination
                neighbor_cost = neighbor_vectors[
                    neighbor_id
                ].get(destination, float("inf"))

                # Bellman-Ford equation:
                # D_x(y) = min(c(x,v) + D_v(y))
                new_cost = link_cost + neighbor_cost

                if new_cost < best_cost:
                    best_cost = new_cost
                    best_next_hop = neighbor_id

        # Compare calculated route against current route
        old_route = routing_table[destination]

        if (
            old_route["cost"] != best_cost
            or old_route["next_hop"] != best_next_hop
        ):
            routing_table[destination] = {
                "next_hop": best_next_hop,
                "cost": best_cost
            }

            updated = True

    return updated


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", required=True, help="Topology file")
    parser.add_argument("-i", required=True, type=int, help="Update interval")

    args = parser.parse_args()

    #prints topology file content and terminal cmd
    '''if args.i <= 0:
        parser.error("Update interval must be positive")

    print("Topology file:", args.t)
    print("Update interval:", args.i)

    with open(args.t, "r") as file:
        print(file.read())
'''
    my_id, num_servers, num_neighbors, servers, neighbors = read_topology(args.t)

    routing_table = initialize_routing_table(
    my_id,
    servers,
    neighbors
    )

    neighbor_vectors = {}

    routing_lock = threading.Lock()

    # Create and bind UDP socket
    sock = initialize_socket(my_id, servers)

    # Start receiving messages in a separate thread
    receiver_thread = threading.Thread(
        target=receive_messages,
        args=(
            sock,
            my_id,
            servers,
            neighbors,
            neighbor_vectors,
            routing_table,
            routing_lock
        ),
        daemon=True
    )

    receiver_thread.start()

    print(
    "\nType 'step' to send a test message.\n"
    "Type 'display' to display routing table\n"
    "Type 'exit' to stop the server.\n"
    )

    try:
        while True:
            command = input("> ").strip().lower()

            if command == "step":
                with routing_lock:
                    send_messages(
                        sock, my_id, servers, neighbors, routing_table
                    )

            elif command == "display":
                display_routing_table(routing_table)

            elif command == "exit":
                break

            else:
                print("Unknown command")

    except (KeyboardInterrupt, EOFError):
        pass

    finally:
        sock.close()
        print(f"Server {my_id} stopped.")

if __name__ == "__main__":
    main()
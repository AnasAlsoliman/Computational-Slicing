import socket
import time
import math
import json
from threading import Thread
from multiprocessing import Process, Manager, Value, Queue
import select
import queue
import csv
import os
from socket import timeout as socket_timeout
import asyncio


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")
            # time.sleep(1)


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_client_lte_info(client_col_address):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["col_ip"] == client_col_address:
            client_lte_ip = node_dict["lte_ip"]
            imsi = node_dict["ue_imsi"]
            return client_lte_ip, node_id, imsi


def unpack_header(message):
    # global traffic_measurements
    # print("message size:", len(message))
    # message_size includes the 30-bytes header.
    timestamp = message[:20]
    timestamp = float(timestamp)
    # current_time = time.time()

    sequence_number = message[20:30]
    # print("new message_size", message_size)
    sequence_number = int(sequence_number)
    # print("sequence_number", sequence_number)

    # traffic_measurements.put([len(message), delay, sequence_number])
    return timestamp, sequence_number
    # return message_size - header_size


def self_close_udp_socket(client_col_ip):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)
    client_receiving_port = 9900 + int(client_imsi[13:])
    closing_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    closing_message = bytes('', 'utf')

    while True:
        sent = closing_socket.sendto(closing_message, (lte_ip, client_receiving_port))
        client_dict = process_dict[client_node_id]
        if client_dict["status"] == "down":
            break
        # time.sleep(1)
    closing_socket.close()


"""
This function can terminate by three different events:
- by detecting client_dict["status"] == "down" which is set by the measurement function.
- by timing out on recvfrom then detecting client_dict["status"] == "down".
- by the dedicated self_close_udp_socket function (which is enough to cover the previous events).
"""
def start_uplink_receiver(client_col_ip):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_col_ip)
    uplink_receiver_port = 9900 + int(client_imsi[13:])
    uplink_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    uplink_receiver_socket.bind((lte_ip, uplink_receiver_port))
    uplink_receiver_socket.settimeout(5)  # Currently not needed.

    # node_dict = {}
    # node_dict["client_node_id"] = client_node_id
    # node_dict["client_col_ip"] = client_col_ip
    # node_dict["client_col_ip"] = client_col_ip
    # clients_dict[client_node_id] = node_dict

    my_slice_queue = slice_1_queue

    counter = 0
    # sequence = 0
    total_received = 0
    current_time = time.time()

    while True:
        try:
            # Currently not needed because now we have self_close_udp_socket.
            # client_dict = process_dict[client_node_id]
            # if client_dict["status"] == "down":
            #     print("Signal from measurement process to close receiver process for node", client_node_id)
            #     break

            data, addr = uplink_receiver_socket.recvfrom(MTU)
            receive_time = time.time()

            if len(data) == 0:
                print("close message received - injection process closed from the client side")
                # To inform the self-closer that the socket is closed successfully.
                client_dict = process_dict[client_node_id]
                client_dict["status"] = "down"
                process_dict[client_node_id] = client_dict
                break

            timestamp, sequence_number = unpack_header(data)
            packet_size = len(data)
            packet_report = [client_node_id, receive_time, timestamp, sequence_number, packet_size]
            my_slice_queue.put(packet_report)

            # total_received = total_received + len(data)
            # counter = counter + 1

            # if (receive_time - current_time) > 1:
            #     current_traffic = total_received * 8 / 1000000
            #     current_traffic = '{0:.3f}'.format(current_traffic)
            #     print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
            #           "seconds from node ", client_node_id, "- current sequence number:", sequence_number)
            #     total_received = 0
            #     current_time = time.time()

        except socket_timeout:
            # client_dict = process_dict[client_node_id]
            # if client_dict["status"] == "down":
            #     print("timeout - closing receiver process for node", client_node_id)
            #     break
            print("Process hung for 5 seconds.")
            # restart_switch.value = True  # Inform the main process to restart me.
            # traffic_socket.close()
            # return
        except KeyboardInterrupt:
            print("\nCtrl+C on receiver process for node", client_node_id)
            break

    print("total received", counter, "messages")
    print("closing down uplink receiver process for node", client_node_id)


# Using TCP on Colosseum's internal network.
def start_measurements_connection(client_socket, client_address):

    client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_address[0])

    while True:
        try:
            message = client_socket.recv(1000)
            if len(message) == 0:
                print("client", client_node_id, "closed the measurement socket")
                self_close_udp_socket(client_address[0])
                break
        except KeyboardInterrupt:
            print("\nCtrl+C on measurement process for node", client_node_id)
            break

    client_socket.shutdown(2)
    client_socket.close()

    # I don't think it is necessary anymore because now we have self_close_udp_socket.
    # client_dict = process_dict[client_node_id]
    # client_dict["status"] = "down"
    # process_dict[client_node_id] = client_dict

    print("closing down measurement process for node", client_node_id)

    # sender_thread = Thread(target=backbone_sender, args=(client_socket, client_address,))
    # sender_thread.start()

    # Current thread is the receiver thread.

    # while True:
    #     try:
    #         client_dict = process_dict[client_node_id]
    #         if client_dict["status"] == "down":
    #             print("timeout - closing receiver process for node", client_node_id)
    #             break
    #         time.sleep(1)
    #     except KeyboardInterrupt:
    #         client_dict = process_dict[client_node_id]
    #         client_dict["status"] = "down"
    #         process_dict[client_node_id] = client_dict
    #         print("\nCtrl+C on receiver process for node", client_node_id)
    #         break

    # sent = client_socket.send(bytes("close", 'utf'))


def start_receiving_connections():

    # A TCP server socket set into the internal Colosseum network.
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        try:
            server_socket.bind((col_ip, col_port))
            server_socket.listen(10)
            print("Starting a TCP server on", col_ip, "at port", col_port)
            break
        except OSError:
            print("Couldn't bind on IP", col_ip, "and port", col_port)

    while True:
        try:
            client_socket, client_address = server_socket.accept()
            print("UE", client_address[0], "connected from port", client_address[1], "on the Colosseum internal network")

            client_lte_ip, client_node_id, client_imsi = get_client_lte_info(client_address[0])
            client_dict = {}
            client_dict["status"] = "up"
            client_dict["internal_socket"] = client_socket
            process_dict[client_node_id] = client_dict

            client_uplink_injection_process = Process(target=start_uplink_receiver, args=(client_address[0],))
            client_uplink_injection_process.start()

            client_measurement_process = Process(target=start_measurements_connection, args=(client_socket, client_address,))
            client_measurement_process.start()

        # This happens when closing a socket on accept().
        except ConnectionAbortedError:
            print("Server socket closed")
            server_socket.close()
            break

        except KeyboardInterrupt:
            print("\nCtrl+C: closing down entire server")
            for node_id, node_dict in process_dict.items():
                node_dict["status"] = "down"
                node_dict["internal_socket"].close()
                # server_socket.shutdown(2)
            server_socket.close()
            break

    print("closing down main server process")


def start_slice_manager(my_slice_id):

    slice_configuration = get_json_file("radio_api/slice_configuration.txt")
    for slice_id, slice_dict in slice_configuration.items():
        if int(slice_id) == my_slice_id:
            housed_nodes = slice_dict["housed_nodes"]
            max_bandwidth = slice_dict["max_bandwidth"]  # in bytes
            break

    # for key, val in slice_dict.items():
    #     print(key, val)
    # print("first node:", housed_nodes[0], "- type:", type(housed_nodes[0]))
    print("max_bandwidth:", max_bandwidth * 8 / 1000000, "Mbps")

    global datasets_queues
    datasets_queues = {}

    number_of_nodes = len(housed_nodes)
    slice_max_buffer_size = max_bandwidth
    node_max_buffer_size = slice_max_buffer_size / number_of_nodes  # Divide the slice buffer equally among slice nodes.
    node_current_buffer_size = 0  # Every node starts with an empty buffer.

    counter = 0
    for node_id in housed_nodes:
        # node_queue = queue.Queue()
        node_id = str(node_id)
        node_dict = {}
        node_dict["my_queue"] = queue.Queue()  # To deliver the packets to the dataset creation thread.
        node_dict["my_buffer"] = node_current_buffer_size  # If the buffer is filled, the next packet will be marked as 'dropped'.
        node_dict["my_id"] = node_id
        datasets_queues[node_id] = node_dict
        client_dataset_thread = Thread(target=create_dataset, args=(node_id, node_dict["my_queue"],))
        client_dataset_thread.start()
        counter += 1
        # print("node_id type", type(node_id))

    print("created", counter, "dataset threads at slice manager", slice_id)

    counter = 0
    skip_counter = 0
    # sequence = 0
    # total_received = 0
    total_pkts = 0
    current_time = time.time()
    last_second = time.time()

    current_slice_buffer = 0  # In bytes.

    while True:
        try:

            # packet_report = slice_1_queue.get(block=False)
            packet_report = slice_1_queue.get()

            client_node_id = packet_report[0]
            receive_time = packet_report[1]
            timestamp = packet_report[2]
            sequence_number = packet_report[3]
            packet_size = packet_report[4]
            delay = '{0:.7f}'.format((receive_time - timestamp) * 1000)  # In millisecond.
            # delay = round(delay, 5)
            # print("sequence_number:", sequence_number, "- delay:", delay)
            # print("client_node_id type", type(client_node_id))

            counter = counter + 1

            passed_time = time.time() - current_time
            current_time = time.time()

            # First step: process/remove bytes from the buffer at the max_bandwidth rate given the passed_time.
            bytes_processed = passed_time * max_bandwidth

            ########## Option 2 ##########
            # # The round-robin process needs a sorted list of buffer sizes to work correctly.
            # node_buffers_list = []
            # current_slice_buffer = 0
            # for node_id, node_dict in datasets_queues.items():
            #     node_id = node_dict["my_id"]
            #     node_current_buffer_size = node_dict["my_buffer"]
            #     node_buffers_list.append([node_id, node_current_buffer_size])
            #     # current_slice_buffer += node_current_buffer_size
            # node_buffers_list.sort(key=lambda x: x[1])  # Sort the nodes based on their buffer sizes.
            #
            # # Allocate resources (process/remove bytes from node buffers) for each node in RR fashion.
            # num_of_nodes = len(node_buffers_list)
            # for node in node_buffers_list:
            #     node_share = math.floor(bytes_processed / num_of_nodes)  # node_share in bytes unit (no fraction of bytes).
            #     if node[1] < node_share:
            #         allocated_resources = node[1]  # The node has less bytes in its buffer than its allocated share.
            #     else:
            #         allocated_resources = node_share
            #     num_of_nodes -= 1
            #     bytes_processed -= allocated_resources
            #
            #     datasets_queues[node[0]]["my_buffer"] -= allocated_resources
            #     current_slice_buffer += datasets_queues[node[0]]["my_buffer"]
            #
            #     # Buffer should never be less than 0 when using RR.
            #     if datasets_queues[node[0]]["my_buffer"] < 0:
            #         print(node[0], "buffer is", datasets_queues[node[0]]["my_buffer"])
            #         break
            #         # datasets_queues[node[0]]["my_buffer"] = 0

            #### Option 2 ####
            current_slice_buffer = current_slice_buffer - bytes_processed
            datasets_queues[client_node_id]["my_buffer"] -= bytes_processed / len(housed_nodes)

            if current_slice_buffer < 0:
                current_slice_buffer = 0
            if datasets_queues[client_node_id]["my_buffer"] < 0:
                datasets_queues[client_node_id]["my_buffer"] = 0
            ####

            # First 'if' for option 1. Second 'if' for option 2.
            if (current_slice_buffer + packet_size) > slice_max_buffer_size:
            # if (datasets_queues[client_node_id]["my_buffer"] + packet_size) > node_max_buffer_size:
                dropped = True
            else:
                datasets_queues[client_node_id]["my_buffer"] += packet_size
                current_slice_buffer += packet_size
                dropped = False
            node_current_buffer_size = datasets_queues[client_node_id]["my_buffer"]

            # Adding additional information to the packet report.
            packet_report.append(delay)
            packet_report.append(dropped)
            packet_report.append(node_current_buffer_size)
            packet_report.append(node_max_buffer_size)
            packet_report.append(current_slice_buffer)
            packet_report.append(slice_max_buffer_size)
            packet_report.append(max_bandwidth)
            packet_report.append(my_slice_id)
            packet_report.append(number_of_nodes)

            datasets_queues[client_node_id]["my_queue"].put(packet_report)

            total_pkts += 1

            # if (current_time - last_second) > 1:
            #     print("- current_slice_buffer in Mbit:",
            #           '{0:.7f}'.format(current_slice_buffer * 8 / 1000000), "skip_counter:", skip_counter,
            #           "- bytes_processed", bytes_processed)
            #     skip_counter = 0
            #     last_second = current_time

        except queue.Empty:
            skip_counter += 1
        except KeyboardInterrupt:
            break

    for node_id in housed_nodes:
        datasets_queues[str(node_id)]["my_queue"].put("close")

    print("closing slice manager", slice_id, "- total_packets:", total_pkts)


def create_dataset(node_id, my_queue):

    ppr_dataset_name = "ue_" + node_id + ".csv"
    ppr_dataset_path = "/root/ue_datasets/"
    ppr_dataset_file = open(ppr_dataset_path + ppr_dataset_name, "w")
    ppr_dataset_writer = csv.writer(ppr_dataset_file)

    ppr_header = []

    ppr_header.append("packet_size")  # In bytes.
    ppr_header.append("timestamp")  # The timestamp of the message creation at the sender.
    ppr_header.append("sequence_number")
    ppr_header.append("inter_arrival_ms")  # The inter-arrival time between this packet and the last received packet.
    ppr_header.append("lost_packets")  # The packets lost in transmission from this packet to the last received packet.
    ppr_header.append("receive_time")  # The time the packet is received at the antenna.
    ppr_header.append("transmission_delay_ms")  # receive_time - timestamp
    ppr_header.append("dropped")  # "1" if the node buffer is full, and "0" otherwise.
    ppr_header.append("buffering_delay_ms")  # receive_time - (the time the packet exited the buffer) ("-1" for dropped packets.
    ppr_header.append("node_current_buffer_size")  # How many bytes in the node's buffer at the moment this packet was received.
    ppr_header.append("node_max_buffer_size")  # Currently does not change once initialized.
    ppr_header.append("slice_current_buffer_size")  # How many bytes in the whole slice buffer at the moment this packet was received.
    ppr_header.append("slice_max_buffer_size")  # Currently does not change once initialized.
    ppr_header.append("node_id")  # Currently does not change once initialized.
    ppr_header.append("slice_id")  # Currently does not change once initialized.
    ppr_header.append("slice_max_bandwidth")  # In Mbps. Currently does not change once initialized.
    ppr_header.append("number_of_users")  # number_of_users in this slice. Currently does not change once initialized.

    ppr_dataset_writer.writerow(ppr_header)

    current_time = time.time()
    beginning_of_time = time.time()
    total_received = 0
    total_dropped = 0
    total_pkt = 0
    pkt_per_second = 0
    skip_counter = 0
    time_left = 1
    total_session = 0
    time_passed = 1

    first_packet = my_queue.get()

    if first_packet == "close":
        return

    client_node_id = first_packet[0]
    previous_packet_arrival = first_packet[1]
    previous_sequence = first_packet[3]

    while True:
        try:
            # packet_report = my_queue.get(block=False)
            packet_report = my_queue.get()

            if packet_report == "close":
                break

            pkt_per_second += 1
            total_pkt += 1

            client_node_id = packet_report[0]
            receive_time = packet_report[1]
            timestamp = packet_report[2]
            sequence_number = packet_report[3]
            packet_size = packet_report[4]
            transmission_delay = packet_report[5]
            dropped = packet_report[6]
            node_current_buffer_size = packet_report[7]
            node_max_buffer_size = packet_report[8]
            current_slice_buffer_size = packet_report[9]
            slice_max_buffer_size = packet_report[10]
            slice_max_bandwidth = packet_report[11]
            slice_id = packet_report[12]
            number_of_nodes = packet_report[13]

            exit_time = time.time()
            # buffering_delay = exit_time - receive_time
            buffering_delay = current_slice_buffer_size / slice_max_bandwidth  # Theoretical buffering time.
            lost_packets = sequence_number - previous_sequence - 1
            inter_arrival = receive_time - previous_packet_arrival

            previous_packet_arrival = receive_time
            previous_sequence = sequence_number

            buffering_delay = '{0:.7f}'.format(buffering_delay * 1000)
            inter_arrival = '{0:.7f}'.format(inter_arrival * 1000)

            assert node_id == client_node_id

            if dropped:
                total_dropped += 1
            else:
                total_received = total_received + packet_size

            if (exit_time - current_time) > 1:
                current_traffic = total_received * 8 / 1000000
                total_session = total_session + current_traffic
                average_throughput = total_session / (time.time() - beginning_of_time)

                time_window = '{0:.7f}'.format(time.time() - current_time)

                current_traffic = float(current_traffic) / float(time_window)
                current_traffic = '{0:.3f}'.format(current_traffic)

                # print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
                #       "seconds from node", client_node_id, "- total_dropped", total_dropped, "- pkt_per_second =",
                #       pkt_per_second, "- skip_counter:", skip_counter)

                if int(client_node_id) == 3:
                    print(current_traffic)

                # print(current_traffic, "Mbps received from node", client_node_id, "average_throughput:", average_throughput)

                total_received = 0
                total_dropped = 0
                pkt_per_second = 0
                skip_counter = 0
                current_time = time.time()
                time_passed += 1

            packet_dataset_entry = []

            packet_dataset_entry.append(packet_size)
            packet_dataset_entry.append(timestamp)
            packet_dataset_entry.append(sequence_number)
            packet_dataset_entry.append(inter_arrival)
            packet_dataset_entry.append(lost_packets)
            packet_dataset_entry.append(receive_time)
            packet_dataset_entry.append(transmission_delay)
            packet_dataset_entry.append(dropped)
            packet_dataset_entry.append(buffering_delay)
            packet_dataset_entry.append(node_current_buffer_size)
            packet_dataset_entry.append(node_max_buffer_size)
            packet_dataset_entry.append(current_slice_buffer_size)
            packet_dataset_entry.append(slice_max_buffer_size)
            packet_dataset_entry.append(client_node_id)
            packet_dataset_entry.append(slice_id)
            packet_dataset_entry.append(slice_max_bandwidth)
            packet_dataset_entry.append(number_of_nodes)

            ppr_dataset_writer.writerow(packet_dataset_entry)

        except queue.Empty:
            skip_counter += 1
            if (time.time() - current_time) > 1:
                current_traffic = total_received * 8 / 1000000
                current_traffic = '{0:.3f}'.format(current_traffic)
                print(current_traffic, "Mbits received within", '{0:.7f}'.format(time.time() - current_time),
                      "seconds from node", client_node_id, "- total_dropped", total_dropped, "- pkt_per_second =",
                      pkt_per_second, "- skip_counter:", skip_counter)
                total_received = 0
                total_dropped = 0
                pkt_per_second = 0
                skip_counter = 0
                current_time = time.time()
        except KeyboardInterrupt:
            break

    print("closing dataset thread for node", node_id, "- total logged packets:", total_pkt)
    ppr_dataset_file.close()


if __name__ == "__main__":

    os.system("rm -rf ue_datasets")
    os.system("mkdir ue_datasets")

    lte_ip = "172.16.0.1"

    col_ip = get_bs_col_ip()
    col_port = 5555

    MTU = 1472  # 1500 - 20 (IP) - 8 (UDP)
    header_size = 30
    MTU_data = 'x' * (MTU - header_size)

    process_dict = Manager().dict()
    clients_dict = Manager().dict()

    slice_1_queue = Queue()
    # slice_2_queue = Queue()
    # slice_3_queue = Queue()

    slice_1_process = Process(target=start_slice_manager, args=(1,))
    # slice_2_process = Process(target=start_slice_manager, args=(2,))
    # slice_3_process = Process(target=start_slice_manager, args=(3,))

    slice_1_process.start()
    # slice_2_process.start()
    # slice_3_process.start()

    start_receiving_connections()

    slice_1_process.join()
    # slice_2_process.join()
    # slice_3_process.join()

    print("terminating server program")

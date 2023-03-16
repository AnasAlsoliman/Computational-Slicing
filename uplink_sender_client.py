
import socket
from socket import timeout as socket_timeout
import time
import json
from multiprocessing import Process, Queue, Value
import queue
import math
import select


def get_injection_file_node(node_id):
    injection_file = open("nodes_config_files/" + str(node_id) + "_traffic_file.txt")
    while True:
        try:
            traffic_parameters = json.load(injection_file)
            break
        except json.decoder.JSONDecodeError:
            print("failed to open traffic file of node", node_id, "we will try again.")
    injection_file.close()
    return traffic_parameters


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open (", json_path, "). Will try again.")


def get_bs_col_ip():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["node_type"] == "bs":
            col_ip = node_dict["col_ip"]
            return col_ip


def get_my_info():
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_dict["this_is_my_node"]:
            return node_dict


def get_node_info(target_node_id):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if int(node_id) == target_node_id:
            return node_dict


def get_injection_parameters(node_id):
    traffic_parameters = get_injection_file_node(node_id)
    packet_size = traffic_parameters["packet_size"]
    desired_bandwidth = traffic_parameters["desired_bandwidth"]
    return packet_size, desired_bandwidth


def create_packet(data, sequence_number):

    padding = "0" * (10 - len(sequence_number))  # sequence_number should be exactly 10 bytes.
    sequence_number = padding + sequence_number

    timestamp = time.time()
    timestamp = str(timestamp)
    padding = "0" * (20 - len(timestamp))  # timestamp should be exactly 20 bytes.
    timestamp = timestamp + padding

    message = bytes(timestamp + sequence_number + data, 'utf')
    return message


def start_uplink_traffic_injection():

    message_size, desired_bandwidth = get_injection_parameters(my_node_id)

    num_of_packets = message_size / MTU  # How many packets we need to send a single message.
    num_of_MTU_packets = math.floor(num_of_packets)  # The last packet is less than MTU size.

    sequence_number = 1
    total_sent = 0  # For print purposes only.
    sleep_time = message_size / desired_bandwidth  # inter-arrival time of messages (not packets).

    print('Message size: ', message_size / 1000, "KB")
    print('Desired bandwidth: ', desired_bandwidth * 8 / 1000000, "Mbps")
    print("Whole message inter-arrivals: ", sleep_time, "second")
    print("uplink_injection_port:", uplink_injection_port)

    one_second = time.time()

    while True:

        try:
            current_time = time.time()

            # The server closed the measurement socket which sets kill_switch to True.
            if kill_switch.value:
                print("server is closed (uplink injection)")
                break

            ############### Start Injection #################
            for i in range(0, num_of_MTU_packets):
                packet = create_packet(MTU_data, str(sequence_number))
                sent = uplink_injection_socket.sendto(packet, (server_lte_ip, uplink_injection_port))
                # sent = send_to_uplink_pd_mc(MTU_data, sequence_number)
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                if sequence_number == 9999999999:
                    sequence_number = 1

            # Both actually works.
            # remaining_bytes = message_size - total_sent
            remaining_bytes = message_size - (MTU * num_of_MTU_packets)

            if remaining_bytes != 0:  # We still have less-than-MTU bytes to send.
                if remaining_bytes <= header_size:  # Minimum packet size is header size.
                    packet = create_packet('', str(sequence_number))
                else:
                    packet = create_packet(MTU_data[:remaining_bytes - header_size], str(sequence_number))
                sent = uplink_injection_socket.sendto(packet, (server_lte_ip, uplink_injection_port))
                total_sent = total_sent + sent
                sequence_number = sequence_number + 1
                if sequence_number == 9999999999:
                    sequence_number = 1
            ################ End Injection #################

            batch = total_sent * 8 / 1000000
            time_window = time.time() - one_second

            if (current_time - one_second) > 1:
                # print("-- sent:", total_sent * 8 / 1000000, "Mbit within", time.time() - one_second)
                print("-- sent:", batch / time_window, "Mbit")
                one_second = time.time()
                total_sent = 0

                new_message_size, new_desired_bandwidth = get_injection_parameters(my_node_id)
                if message_size == new_message_size and desired_bandwidth == new_desired_bandwidth:
                    pass  # No change in traffic profile.
                else:
                    # print("new injection parameters for node", client_node_id, "- message_size", new_message_size, "desired_bandwidth", new_desired_bandwidth)
                    message_size = new_message_size
                    desired_bandwidth = new_desired_bandwidth
                    sleep_time = message_size / desired_bandwidth
                    num_of_packets = message_size / MTU
                    num_of_MTU_packets = math.floor(num_of_packets)

                    # Inter-arrival of packets cannot exceed 1 second.
                    if sleep_time > 1:
                        sleep_time = 1

            # print("----------------------------")
            wasted_time = time.time() - current_time
            time.sleep(sleep_time - wasted_time)
            # time.sleep(sleep_time)

        # This happens when trying to sendto() a non-empty buffer.
        # This will never happen if the socket is not set to non-blocking.
        except BlockingIOError:
            select.select([], [uplink_injection_socket], [])
            print("buffer is full (the remaining of the message will be lost)")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\nctrl+C detected on injection process for node", my_node_id)
            break

    uplink_injection_socket.close()
    # support_socket.close()
    print("existing injection process for client", my_node_id)


def start_measurement():

    while True:
        try:
            measurements_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            measurements_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            measurements_socket.connect((server_col_ip, server_col_port))
            break
        except ConnectionRefusedError:
            if restart_switch.value or kill_switch.value:
                print("Measurement server already closed. Exiting process.")
                return
            print("We will try again shortly (measurements_socket)")
            time.sleep(1)
        except OSError:
            print("Server LTE IP is not ready yet (measurements_socket)")
            time.sleep(1)

    print("connected to main server for measurement on Colosseum IP", server_col_ip)

    traffic_uplink_injection_process = Process(target=start_uplink_traffic_injection)
    traffic_uplink_injection_process.start()

    # TODO: measurement channel is not complete yet.
    while True:
        try:
            message = measurements_socket.recv(1000)
            if len(message) == 0:
                print("server closed the measurement socket")
                break
        except KeyboardInterrupt:
            print("\nCtrl+C on measurement process")
            break
    measurements_socket.shutdown(2)
    measurements_socket.close()
    kill_switch.value = True


if __name__ == "__main__":

    node_dict = get_my_info()
    my_imsi = node_dict["ue_imsi"]
    my_lte_ip = node_dict["lte_ip"]
    my_col_ip = node_dict["col_ip"]
    my_node_id = node_dict["node_id"]

    # Every UE will have a dedicated UDP process/port using UE's last 2 IMSI digits.
    uplink_injection_port = 9900 + int(my_imsi[13:])
    uplink_injection_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP injection socket.

    server_lte_ip = "172.16.0.1"

    server_col_ip = get_bs_col_ip()
    server_col_port = 5555

    restart_switch = Value('i')
    restart_switch.value = False

    kill_switch = Value('i')
    kill_switch.value = False

    # 1472 makes sure to fit our UDP packet to exactly a single MAC-layer frame which is 1500 bytes.
    MTU = 1472  # 1472 + IP 20-bytes + UDP 8-bytes = 1500 bytes.
    header_size = 30
    MTU_data = 'x' * (MTU - header_size)

    while True:

        try:
            traffic_measurements = Queue()
            restart_switch.value = False

            measurements_socket_process = Process(target=start_measurement)
            measurements_socket_process.start()
            measurements_socket_process.join()

            # Exiting the entire program when the server closes.
            if kill_switch.value:
                print("terminating entire client process")
                break

            # Traffic process got hung so we need to restart the process.
            print("refreshing client process")

        except KeyboardInterrupt:
            # A better option is to close the recv() socket in case of the server is connected but idle (although the
            # server is designed to never be idle).
            kill_switch.value = True
            # delay_measurements.put("exit")  # I don't think it is really needed.
            break

    print("terminating program...")




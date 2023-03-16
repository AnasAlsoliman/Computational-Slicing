
import argparse
import json
import csv


def get_json_file(json_path):
    while True:
        try:
            config_file = open(json_path)
            node_parameters = json.load(config_file)
            config_file.close()
            return node_parameters
        except json.decoder.JSONDecodeError:
            print("failed to open", json_path, ". Will try again.")


def get_client_dict(target_node_id):
    nodes_parameters = get_json_file("nodes_parameters_file.txt")
    for node_id, node_dict in nodes_parameters.items():
        if node_id == str(target_node_id):
            col_ip = node_dict["col_ip"]
            imsi = node_dict["ue_imsi"]
            return col_ip, imsi


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--node_idx', type=int, nargs='+')
    args = parser.parse_args()

    if args.node_idx:
        node_id = args.node_idx[0]
        print("Node ID:", args.node_idx[0])

    col_ip, imsi = get_client_dict(node_id)

    scope_dataset_name = imsi[2:] + "_metrics.csv"
    scope_dataset_path = "/root/radio_code/scope_config/metrics/csv/"
    scope_dataset_file = open(scope_dataset_path + scope_dataset_name)
    scope_dataset_reader = csv.reader(scope_dataset_file)

    scope_header = next(scope_dataset_reader)

    # print("scope_header:\n", scope_header)

    # for feature in scope_header:
    #     print(feature)
    # print("-----------------")

    for i in range(0, len(scope_header)):
        print(i, scope_header[i])
    print("-----------------")

    while True:

        try:
            # If the next line fails, the rest of the "try" block will be skipped.
            scope_entry = next(scope_dataset_reader)

            # "tx_brate downlink [Mbps]" -> 14
            # "rx_brate uplink [Mbps]"   -> 22
            # "sum_requested_prbs"       -> 29
            # "sum_granted_prbs"         -> 30

            # print("tx_brate downlink [Mbps]", scope_entry[14], "- rx_brate uplink [Mbps]", scope_entry[22], "- sum_requested_prbs", scope_entry[29], "sum_granted_prbs", scope_entry[30])
            print("ul_rssi", scope_entry[25], "- ul_sinr", scope_entry[26], "- dl_ri", scope_entry[33], "- tx_errors downlink (%)", scope_entry[16])

        # No more new scope entries. The scope dataset gets a new entry every 250ms.
        except StopIteration:
            pass
            # print("waiting for a new scope dataset entry")
            # time.sleep(0.001)

        # client_col_address is removed by recv_client_measurements due to socket closure.
        # Update: this catch is no longer needed (will remove later).
        except KeyError:
            print("Exiting scope reader for node", node_id)
            scope_dataset_file.close()
            break
        except KeyboardInterrupt:
            print("\nCtrl+C detected")
            scope_dataset_file.close()
            break


import pdb

from flask import Flask, request
import requests
import sys
import random
import time
import torch
import os
import binascii
import copy

import json
from hashlib import sha256
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

class Block:
    def __init__(self, idx, transactions=None, block_generation_time=None, previous_hash=None, nonce=0,
                 block_hash=None):
        self._idx = idx
        self._transactions = transactions or []
        self._block_generation_time = block_generation_time
        self._previous_hash = previous_hash
        self._nonce = nonce
        # the hash of the current block, calculated by compute_hash
        self._block_hash = block_hash

    def compute_hash(self, hash_previous_block=False):
        if hash_previous_block:
            block_content = self.__dict__
        else:
            # = self.__dict__ is even a shallow copy...
            block_content = copy.deepcopy(self.__dict__)
            block_content['_block_hash'] = None
        block_content = json.dumps(block_content, sort_keys=True)
        return sha256(block_content.encode()).hexdigest()

    def set_hash(self):
        self._block_hash = self.compute_hash()

    def nonce_increment(self):
        self._nonce += 1

    def get_block_hash(self):
        return self._block_hash

    def get_previous_hash(self):
        return self._previous_hash

    def get_block_idx(self):
        return self._idx

    def get_transactions(self):
        return self._transactions

    # setters
    def set_previous_hash(self, hash_to_set):
        self._previous_hash = hash_to_set

    def add_verified_transaction(self, transaction):
        # after verified in cross_verification()
        self._transactions.append(transaction)

    def set_block_generation_time(self):
        self._block_generation_time = time.time()


class Blockchain:
    # for PoW
    difficulty = 2

    def __init__(self):
        self._chain = []

    def get_chain_length(self):
        return len(self._chain)

    def get_last_block(self):
        if len(self._chain) > 0:
            return self._chain[-1]
        else:
            return None

    def append_block(self, block):
        self._chain.append(block)


class Worker:
    def __init__(self, idx):
        self._idx = idx
        self._is_miner = False
        self._ip_and_port = None
        self._blockchain = Blockchain()
        self._data = []
        # weight dimentionality has to be the same as the dim of the data column vector
        self._global_weight_vector = None
        # self._global_gradients = None
        self._step_size = None
        # data dimensionality has to be predefined as an positive integer
        self._data_dim = None
        # sample size(Ni)
        self._sample_size = None
        self._rewards = 0
        self._signal = 0

    ''' getters '''

    def get_idx(self):
        return self._idx

    def get_blockchain(self):
        return self._blockchain

    def get_current_epoch(self):
        return self._blockchain.get_chain_length() + 1

    def get_data(self):
        return self._data

    def get_ip_and_port(self):
        return self._ip_and_port

    # get global_weight_vector, used while being the register_with node to sync with the registrant node
    def get_global_weight_vector(self):
        return self._global_weight_vector

    ''' setters '''

    def get_rewards(self, rewards):
        self._rewards += rewards

    def set_signal(self, signal):
        self._signal = signal
        print("miner finished broadcast")

    def get_signal(self):
        return self._signal

    def set_data_dim(self, data_dim):
        self._data_dim = data_dim

    # set the consensused blockchain
    def set_blockchain(self, blockchain):
        self._blockchain = blockchain

    def is_miner(self):
        return self._is_miner

    def set_ip_and_port(self, ip_and_port):
        self._ip_and_port = ip_and_port

    ''' Functions for Workers '''

    def worker_set_sample_size(self, sample_size):
        self._sample_size = sample_size

    def worker_set_step_size(self, step_size):
        if step_size <= 0:
            print("Step size has to be positive.")
        else:
            self._step_size = step_size

    def worker_generate_dummy_data(self):

        self._sample_size = random.randint(5, 10)

        r1, r2 = 0, 2
        if not self._data:
            self.expected_w = torch.tensor([[3.0], [7.0], [12.0]])
            for _ in range(self._sample_size):
                x_tensor = (r1 - r2) * torch.rand(self._data_dim, 1) + r2
                y_tensor = self.expected_w.t() @ x_tensor
                self._data.append({'x': x_tensor, 'y': y_tensor})
        else:
            print(
                "The data of this worker has already been initialized. Changing data is not currently implemented in this version.")

    def worker_init_global_weihgt(self):
        if self._is_miner:
            print("Miner does not set weight values")
        else:
            if self._global_weight_vector is None:

                self._global_weight_vector = torch.zeros(self._data_dim, 1)
            else:
                print("This function shouldn't be called.")

    def worker_associate_miner_with_same_epoch(self):
        if self._is_miner:
            print("Miner does not associate with another miner.")
            return None
        else:
            potential_new_peers = set()
            miner_nodes = set()
            for node in peers:
                response = requests.get(f'{node}/get_role')
                if response.status_code == 200:
                    if response.text == 'Miner':
                        response2 = requests.get(f'{node}/get_miner_comm_round')
                        if response2.status_code == 200:
                            if int(response2.text) == self.get_current_epoch():
                                miner_nodes.add(node)
                                # side action - update (worker) peers from all miners
                                # TODO, actually, though worker peer, may also update its peer list
                                response3 = requests.get(f'{node}/get_peers')
                                if response3.status_code == 200:
                                    potential_new_peers.update(response3.json()['peers'])
                else:
                    return "Error in worker_associate_miner_with_same_epoch()", response.status_code
            peers.update(potential_new_peers)
            try:
                peers.remove(self._ip_and_port)
            except:
                pass
        if miner_nodes:
            return random.sample(miner_nodes, 1)[0]
        else:
            return None

    def worker_upload_to_miner(self, upload, miner_address):
        if self._is_miner:
            print("Worker does not accept other workers' updates directly")
        else:
            checked = False
            # check if this node is still a miner
            response = requests.get(f'{miner_address}/get_role')
            if response.status_code == 200:
                if response.text == 'Miner':
                    # check if worker and miner are in the same epoch
                    response_epoch = requests.get(f'{miner_address}/get_miner_comm_round')
                    if response_epoch.status_code == 200:
                        miner_epoch = int(response_epoch.text)
                        if miner_epoch == self.get_current_epoch():
                            checked = True
                        else:
                            pass
            if checked:

                response_miner_accepting = requests.get(f'{miner_address}/within_miner_wait_time')
                if response_miner_accepting.status_code == 200:
                    if response_miner_accepting.text == "True":
                        upload['this_worker_address'] = self._ip_and_port
                        miner_upload_endpoint = f"{miner_address}/new_transaction"
                        requests.post(miner_upload_endpoint,
                                      data=json.dumps(upload),
                                      headers={'Content-type': 'application/json'})
                    else:
                        return "Not within miner waiting time."
                else:
                    return "Error getting miner waiting status", response_miner_accepting.status_code

    def worker_receive_rewards_from_miner(self, rewards):
        print(f"Before rewarded, this worker has rewards {self._rewards}.")
        self.get_rewards(rewards)
        print(f"After rewarded, this worker has rewards {self._rewards}.\n")

    def worker_receive_signal_from_miner(self, signal):
        print(f"Before received signal, this worker has signal {self._signal}.")
        self.set_signal(signal)
        print(f"After received signal, this worker has signal {self._signal}.\n")

    def worker_local_update_linear_regresssion(self):
        if self._is_miner:
            print("Miner does not perfrom gradient calculations.")
        else:
            start_time = time.time()
            feature_gradients_tensor = torch.zeros(self._data_dim, 1)
            for data_point in self._data:
                difference_btw_hypothesis_and_true_label = data_point['x'].t() @ self._global_weight_vector - \
                                                           data_point['y']

                feature_gradients_tensor += difference_btw_hypothesis_and_true_label * data_point['x']
            feature_gradients_tensor /= len(self._data)

        print(f"Current global_weights: {self._global_weight_vector}")
        print(
            f"Abs difference from expected weights({self.expected_w}): {abs(self.expected_w - self._global_weight_vector)}")
        return {"worker_id": self._idx, "worker_ip": self._ip_and_port,
                "feature_gradients": {"feature_gradients_list": feature_gradients_tensor.tolist(),
                                      "tensor_type": feature_gradients_tensor.type()},
                "computation_time": time.time() - start_time}


    def worker_local_update_SVRG(self):
        if self._is_miner:
            print("Miner does not perfrom gradient calculations.")
        else:
            global_gradients_per_data_point = []
            local_weight = self._global_weight_vector
            last_block = self._blockchain.get_last_block()
            if last_block is not None:
                transactions = last_block.get_transactions()
                tensor_accumulator = torch.zeros_like(self._global_weight_vector)
                for update_per_device in transactions:
                    for data_point_gradient in update_per_device['global_gradients_per_data_point']:
                        data_point_gradient_list = data_point_gradient['update_tensor_to_list']
                        data_point_gradient_tensor_type = data_point_gradient['tensor_type']
                        data_point_gradient_tensor = getattr(torch, data_point_gradient_tensor_type[6:])(
                            data_point_gradient_list)
                        tensor_accumulator += data_point_gradient_tensor
                num_of_device_updates = len(transactions)
                delta_f_wl = tensor_accumulator / (num_of_device_updates * self._sample_size)
            else:
                delta_f_wl = torch.zeros_like(self._global_weight_vector)
                for data_point in self._data:
                    local_weight_track_grad = local_weight.clone().detach().requires_grad_(True)
                    fk_wl = (data_point['x'].t() @ local_weight_track_grad - data_point['y']) ** 2 / 2
                    fk_wl.backward()
                    delta_f_wl += local_weight_track_grad.grad
                delta_f_wl /= self._sample_size
            start_time = time.time()
            for data_point in self._data:
                local_weight_track_grad = local_weight.clone().detach().requires_grad_(True)

                fk_wil = (data_point['x'].t() @ local_weight_track_grad - data_point['y']) ** 2 / 2

                fk_wil.backward()
                delta_fk_wil = local_weight_track_grad.grad

                last_global_weight_track_grad = self._global_weight_vector.clone().detach().requires_grad_(True)

                fk_wl = (data_point['x'].t() @ last_global_weight_track_grad - data_point['y']) ** 2 / 2

                fk_wl.backward()
                delta_fk_wl = last_global_weight_track_grad.grad

                global_gradients_per_data_point.append(
                    {"update_tensor_to_list": delta_fk_wl.tolist(), "tensor_type": delta_fk_wl.type()})
                # calculate local update
                local_weight = local_weight - (self._step_size / len(self._data)) * (
                            delta_fk_wil - delta_fk_wl + delta_f_wl)

            # worker_id and worker_ip is not required to be recorded to the block. Just for debugging purpose
            return {"worker_id": self._idx, "worker_ip": self._ip_and_port,
                    "local_weight_update": {"update_tensor_to_list": local_weight.tolist(),
                                            "tensor_type": local_weight.type()},
                    "global_gradients_per_data_point": global_gradients_per_data_point,
                    "computation_time": time.time() - start_time}

    def worker_global_update_linear_regression(self):
        print("This worker is performing global updates...")
        # alpha
        learning_rate = 0.1
        transactions_in_downloaded_block = self._blockchain.get_last_block().get_transactions()
        print("transactions_in_downloaded_block", transactions_in_downloaded_block)
        feature_gradients_tensor_accumulator = torch.zeros_like(self._global_weight_vector)
        num_of_device_updates = 0
        for update in transactions_in_downloaded_block:
            num_of_device_updates += 1
            feature_gradients_list = update["feature_gradients"]["feature_gradients_list"]
            feature_gradients_tensor_type = update["feature_gradients"]["tensor_type"]
            feature_gradients_tensor = getattr(torch, feature_gradients_tensor_type[6:])(feature_gradients_list)
            feature_gradients_tensor_accumulator += feature_gradients_tensor
        # perform global updates by gradient decent
        self._global_weight_vector -= learning_rate * feature_gradients_tensor_accumulator / num_of_device_updates
        print('updated self._global_weight_vector', self._global_weight_vector)
        print('abs difference from expected weights', abs(self._global_weight_vector - self.expected_w))

        with open(f'./convergence_logs/updated_weights_{self._idx}.txt',
                  "a") as myfile:
            myfile.write(str(self._global_weight_vector) + '\n')
        with open(f'./convergence_logs/weights_diff_{self._idx}.txt',
                  "a") as myfile:
            myfile.write(str(abs(self._global_weight_vector - self.expected_w)) + '\n')

        print()
        for data_point_iter in range(len(self._data)):
            data_point = self._data[data_point_iter]
            print(
                f"For datapoint {data_point_iter}, abs difference from true label: {abs(self._global_weight_vector.t() @ data_point['x'] - data_point['y'])}")
            with open(
                    f'./convergence_logs/prediction_diff_point_{self._idx}_{data_point_iter + 1}.txt',
                    "a") as myfile:
                myfile.write(str(abs(self._global_weight_vector.t() @ data_point['x'] - data_point['y'])) + '\n')
        print("====================")
        print("Global Update Done.")
        print("Press ENTER to continue to the next communication round...")

    def worker_global_update_SVRG(self):
        print("This worker is performing global updates...")
        transactions_in_downloaded_block = self._blockchain.get_last_block().get_transactions()
        print("transactions_in_downloaded_block", transactions_in_downloaded_block)
        Ni = SAMPLE_SIZE
        Nd = len(transactions_in_downloaded_block)
        Ns = Nd * Ni
        global_weight_tensor_accumulator = torch.zeros_like(self._global_weight_vector)
        for update in transactions_in_downloaded_block:
            updated_weigts_list = update["local_weight_update"]["update_tensor_to_list"]
            updated_weigts_tensor_type = update["local_weight_update"]["tensor_type"]
            updated_weigts_tensor = getattr(torch, updated_weigts_tensor_type[6:])(updated_weigts_list)
            print("updated_weigts_tensor", updated_weigts_tensor)
            global_weight_tensor_accumulator += (Ni / Ns) * (updated_weigts_tensor - self._global_weight_vector)
        self._global_weight_vector += global_weight_tensor_accumulator
        print('self._global_weight_vector', self._global_weight_vector)
        print("Global Update Done.")

    ''' Common Methods '''

    # including adding the genesis block
    def worker_add_block(self, block_to_add, pow_proof):
        last_block = self._blockchain.get_last_block()
        if last_block is not None:
            last_block_hash = last_block.compute_hash(hash_previous_block=True)
            if block_to_add.get_previous_hash() != last_block_hash:
                return False
            if not self.check_pow_proof(block_to_add, pow_proof):
                return False
            block_to_add.set_hash()
            self._blockchain.append_block(block_to_add)
            return True
        else:
            if not self.check_pow_proof(block_to_add, pow_proof):
                return False
            # add genesis block
            block_to_add.set_hash()
            self._blockchain.append_block(block_to_add)
            return True

    @staticmethod
    def check_pow_proof(block_to_check, pow_proof):
        return pow_proof.startswith('0' * Blockchain.difficulty) and pow_proof == block_to_check.compute_hash()

    @classmethod
    def check_chain_validity(cls, chain_to_check):
        chain_len = chain_to_check.get_chain_length()
        if chain_len == 0:
            pass
        elif chain_len == 1:
            pass
        else:
            for block in chain_to_check[1:]:
                if cls.check_pow_proof(block, block.get_block_hash()) and block.get_previous_hash == chain_to_check[
                    chain_to_check.index(block) - 1].compute_hash(hash_previous_block=True):
                    pass
                else:
                    return False
        return True

    def consensus(self):

        longest_chain = None
        chain_len = self._blockchain.get_chain_length()

        for node in peers:
            response = requests.get(f'{node}/get_chain_meta')
            length = response.json()['length']
            chain = response.json()['chain']
            if length > chain_len and self.check_chain_validity(chain):
                # Longer valid chain found!
                chain_len = length
                longest_chain = chain

        if longest_chain:
            self._blockchain._chain = longest_chain
            return True

        return False


app = Flask(__name__)

DATA_DIM = 3  # MUST BE CONSISTENT ACROSS ALL WORKERS
SAMPLE_SIZE = 2  # not necessarily consistent
STEP_SIZE = 1
EPSILON = 0.02

PROMPT = ">>>"
peers = set()

device = Worker(binascii.b2a_hex(os.urandom(4)).decode('utf-8'))


@app.route('/get_role', methods=['GET'])
def return_role():
    return "Worker"

@app.route('/get_worker_data', methods=['GET'])
def return_data():
    json.dumps({"data": device.get_data()})

@app.route('/get_worker_epoch', methods=['GET'])
def get_worker_epoch():
    if not device.is_miner():
        return str(device.get_current_epoch())
    else:
        # TODO make return more reasonable
        return "error"

@app.route('/')
def runApp():
    app.debug = True
    print(f"\n==================")
    print(f"|  Demo  |")
    print(f"==================\n")

    print(f"{PROMPT} Device is setting data dimensionality {DATA_DIM}")
    device.set_data_dim(DATA_DIM)
    print(f"{PROMPT} Device is setting sample size {SAMPLE_SIZE}")
    device.worker_set_sample_size(SAMPLE_SIZE)
    print(f"{PROMPT} Step size set to {STEP_SIZE}")
    device.worker_set_step_size(STEP_SIZE)
    print(f"{PROMPT} Worker set global_weight_to_all_0s.")
    device.worker_init_global_weihgt()
    print(f"{PROMPT} Device is generating the dummy data.\n")
    print(f"Dummy data generated.")
    device.worker_generate_dummy_data()

    epochs = 0
    while epochs < 150:
        print("number of epochs", epochs)
        # if epochs > 1:
        #     while (True):
        #         signal = device.get_signal()
        #         print ("wait miner finish broadcast", signal)
        #         if signal == 1:
        #             break
        signal = device.get_signal()
        print("wait miner finish broadcast", signal)
        time.sleep(15)
        print(f"\nStarting communication round {device.get_current_epoch()}...\n")
        print(f"{PROMPT} This is workder with ID {device.get_idx()}")

        print(f"{PROMPT} Worker is performing Step1 - local update...\n")

        upload = device.worker_local_update_linear_regresssion()

        miner_address = device.worker_associate_miner_with_same_epoch()

        if miner_address is not None:
            print(f"{PROMPT} This workder {device.get_ip_and_port()}({device.get_idx()}) now assigned to miner with address {miner_address}.\n")
            device.worker_upload_to_miner(upload, miner_address)
            time.sleep(5)
        epochs += 1


@app.route('/get_rewards_from_miner', methods=['POST'])
def get_rewards_from_miner():
    received_rewards = request.get_json()['rewards']
    print(
        f"\nThis worker received self verification rewards {received_rewards} from the associated miner {request.get_json()['miner_ip']}({request.get_json()['miner_id']})")
    device.worker_receive_rewards_from_miner(received_rewards)
    return "Success", 200


@app.route('/get_signal_from_miner', methods=('GET', 'POST'))
def get_signal_from_miner():
    received_signal = request.get_json()['signal']
    print(
        f"\nThis worker received self verification rewards {received_signal} from the associated miner {request.get_json()['miner_ip']}({request.get_json()['miner_id']})")
    device.worker_receive_signal_from_miner(received_signal)
    return "Success", 200


@app.route('/download_block_from_miner', methods=['POST'])
def download_block_from_miner():
    print(
        f"\nReceived downloaded block from the associated miner {request.get_json()['miner_ip']}({request.get_json()['miner_id']})")
    downloaded_block = request.get_json()["block_to_download"]
    pow_proof = request.get_json()["pow_proof"]
    rebuilt_downloaded_block = Block(downloaded_block["_idx"],
                                     downloaded_block["_transactions"],
                                     downloaded_block["_block_generation_time"],
                                     downloaded_block["_previous_hash"],
                                     downloaded_block["_nonce"])

    added = device.worker_add_block(rebuilt_downloaded_block, pow_proof)
    if added:
        # device.worker_global_update_SVRG()
        device.worker_global_update_linear_regression()
        return "Success", 201
    else:
        return "Nah"


@app.route('/chain', methods=['GET'])
def display_chain():
    chain = json.loads(query_blockchain())["chain"]
    print("\nChain info requested and returned -")
    for block_iter in range(len(chain)):
        block_id_to_print = f"Block #{block_iter + 1}"
        print()
        print('=' * len(block_id_to_print))
        print(block_id_to_print)
        print('=' * len(block_id_to_print))
        block = chain[block_iter]
        # print("_idx", block["_idx"])
        for tx_iter in range(len(block["_transactions"])):
            print(f"\nTransaction {tx_iter + 1}\n", block["_transactions"][tx_iter], "\n")
        print("_block_generation_time", block["_block_generation_time"])
        print("_previous_hash", block["_previous_hash"])
        print("_nonce", block["_nonce"])
        print("_block_hash", block["_block_hash"])
    return "Chain Returned in Port Console"


@app.route('/get_chain_meta', methods=['GET'])
def query_blockchain():
    chain_data = []
    for block in device.get_blockchain()._chain:
        chain_data.append(block.__dict__)
    return json.dumps({"length": len(chain_data),
                       "chain": chain_data,
                       "peers": list(peers)})


@app.route('/get_peers', methods=['GET'])
def query_peers():
    return json.dumps({"peers": list(peers)})


# TODO helper function used in register_with_existing_node() only while registering node
def sync_chain_from_dump(chain_dump):
    for block_data in chain_dump:

        block = Block(block_data["_idx"],
                      block_data["_transactions"],
                      block_data["_block_generation_time"],
                      block_data["_previous_hash"],
                      block_data["_nonce"])
        pow_proof = block_data['_block_hash']
        added = device.worker_add_block(block, pow_proof)
        if not added:
            raise Exception("The chain dump is tampered!!")
        else:
            pass

@app.route('/register_node', methods=['POST'])
def register_new_peers():
    registrant_node_address = request.get_json()["registrant_node_address"]
    if not registrant_node_address:
        return "Invalid data", 400

    transferred_this_node_address = request.get_json()["registrar_node_address"]
    if device.get_ip_and_port() == None:
        device.set_ip_and_port(transferred_this_node_address)
    if device.get_ip_and_port() != transferred_this_node_address:
        return "This should never happen"

    peers.add(registrant_node_address)
    return query_blockchain()


@app.route('/register_with', methods=['POST'])
def register_with_existing_node():
    device.set_ip_and_port(request.host_url[:-1])

    registrar_node_address = request.get_json()["registrar_node_address"]
    if not registrar_node_address:
        return "Invalid request - must specify a registrar_node_address!", 400
    data = {"registrant_node_address": request.host_url[:-1], "registrar_node_address": registrar_node_address}
    headers = {'Content-Type': "application/json"}

    response = requests.post(registrar_node_address + "/register_node", data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        global peers
        peers.add(registrar_node_address)
        chain_data_dump = response.json()['chain']
        sync_chain_from_dump(chain_data_dump)
        peers.update(response.json()['peers'])
        try:
            peers.remove(device.get_ip_and_port())
        except:
            pass
        return "Registration successful", 200
    else:
        return "weird"

@app.route('/debug_peers', methods=['GET'])
def debug_peers():
    return repr(peers)

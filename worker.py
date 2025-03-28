import http.client
from collections import defaultdict
import string
import time
import json
import os

class Worker:
    def __init__(self):
        while True:
            try:
                self.conn = http.client.HTTPConnection("127.0.0.1", 8080)
                self.N, self.M = self.get_params_from_driver()
                self.letter_to_reduce_id = self.get_letter_mapping()
                print("Connected to the server")
                break
            except ConnectionError:
                print("Connection failed. Retrying in 5 seconds...")
                time.sleep(5)

    def get_params_from_driver(self):
        self.conn.request("GET", "/parameters")
        response = self.conn.getresponse()
        parameters = json.loads(response.read().decode())
        return parameters["N"], parameters["M"]

    def get_letter_mapping(self):
        # mapping starts from zero ('a': 0, 'b': 1, ...)
        alphabet = 'abcdefghijklmnopqrstuvwxyz'
        mapped_values = {letter: (ord(letter) - ord('a')) % self.M for letter in alphabet}
        return mapped_values

    def send_get_request(self, url):
        self.conn.request("GET", url)

    def send_post_request(self, url, data):
        json_data = json.dumps(data)
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(json_data))
        }
        self.conn.request("POST", url, body=json_data, headers=headers)
        self.conn.close()

    def map_task(self, task_id, input_filenames):
        word_count = defaultdict(int)
        for input_filename in input_filenames:
            word_count_single = self.count_words(input_filename)
            for key, value in word_count_single.items():
                word_count[key] += value

        # time.sleep(15)
        self.create_folder_if_needed('./outputs_tmp')
        output_filenames = [f'./outputs_tmp/mp-{task_id}-{reduce_id}.txt' for reduce_id in range(self.M)]
        opened_files = [open(output_filename, "w") for output_filename in output_filenames]
        # file index = number of reduce task ('a' should be written into 0-indexed file)

        for word, count in word_count.items():
            reduce_id = self.letter_to_reduce_id[word[0]]
            opened_files[reduce_id].write(f'{word} {count}\n')

        for opened_file in opened_files:
            opened_file.close()

        post_data = {'task_id': task_id}
        self.send_post_request('/map', post_data)

    @staticmethod
    def count_words(filename):
        word_count = defaultdict(int)

        with open(filename, 'r') as file:
            content = file.read()
            words = content.split()

            for word in words:
                # Remove punctuation and convert to lowercase for consistent counting
                word = word.strip(string.punctuation).lower()
                if word.isalpha():
                    word_count[word] += 1

        return word_count

    def reduce_task(self, task_id, input_filenames):
        word_count = defaultdict(int)
        for input_filename in input_filenames:
            with open(input_filename, "r") as file:
                for line in file:
                    word, number = line.split()
                    word_count[word] += int(number)

        self.create_folder_if_needed('./outputs')
        output_filename = f'./outputs/output-{task_id}.txt'
        word_count_sorted = dict(sorted(word_count.items()))
        with open(output_filename, "w") as file:
            for key, value in word_count_sorted.items():
                line = f"{key} {value}\n"
                file.write(line)

        post_data = {'task_id': task_id}
        self.send_post_request('/reduce', post_data)

    @staticmethod
    def create_folder_if_needed(path):
        if not os.path.exists(path):
            os.makedirs(path)


if __name__ == "__main__":
    worker = Worker()
    while True:

        user_input = input("Enter your HTTP command "
                           "(GET map, GET reduce):\n")
        parts = user_input.split()

        if parts[0] == 'GET':
            http_method = parts[0].upper()
            url = "/" + parts[1]

            worker.send_get_request(url)  # Send the HTTP request
            response = worker.conn.getresponse()

            status_code = response.status
            if status_code == 200:
                received_data = json.loads(response.read().decode())
                received_task_id = received_data['task_id']
                received_filenames = received_data['filenames']
                if url == '/map':
                    print(f'Worker received files {received_filenames}\n')
                    worker.map_task(received_task_id, received_filenames)
                elif url == '/reduce':
                    print(f'Worker received files {received_filenames}\n')
                    worker.reduce_task(received_task_id, received_filenames)
                else:
                    print("Bad TASK!\n")
            elif status_code == 300 and url == '/map':
                print(f"All map tasks are finished!\n")
            elif status_code == 303 and url == '/reduce':
                print(f"Not all map tasks are finished!\n")
            elif status_code == 300 and url == '/reduce':
                print(f"All reduce tasks are finished!")
                print(f"The program is finished!\n")
                break
            else:
                print("Wrong endpoint!\n")

        else:
            print("Invalid input format. Please provide a valid HTTP command.")

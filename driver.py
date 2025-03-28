import json
import multiprocessing
import os
import argparse
import time

from collections import defaultdict

from http.server import BaseHTTPRequestHandler, HTTPServer


class MyRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/parameters':
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            response_data = json.dumps({
                "N": self.server.N,
                "M": self.server.M
            })

            self.wfile.write(response_data.encode())

        elif self.path == "/map":
            if self.server.current_map_task.value < self.server.N:
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()

                response_data = json.dumps({
                    'task_id': self.server.current_map_task.value,
                    'filenames': self.server.map_task_to_files[self.server.current_map_task.value]
                })

                with self.server.locker:
                    self.server.map_task_to_files[self.server.current_map_task.value] = 'ongoing'
                    self.server.current_map_task.value += 1

                self.wfile.write(response_data.encode())
            else:
                self.send_response(300)
                self.send_header("Content-type", "plain/text")
                self.end_headers()

                self.wfile.write(b'All tasks are distributed for map\n')

        elif self.path == '/reduce':
            if self.server.current_map_task.value != self.server.N:
                self.send_response(303)
                self.send_header("Content-type", "plain/text")
                self.end_headers()

                self.wfile.write(b'Map tasks are not yet completed\n')

            elif self.server.current_reduce_task.value < self.server.M:  # TODO
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()

                response_data = json.dumps({
                    'task_id': self.server.current_reduce_task.value,
                    'filenames': self.server.reduce_task_to_files[self.server.current_reduce_task.value]
                })

                with self.server.locker:
                    self.server.reduce_task_to_files[self.server.current_reduce_task.value] = 'ongoing'
                    self.server.current_reduce_task.value += 1

                self.wfile.write(response_data.encode())
            else:
                self.send_response(300)
                self.send_header("Content-type", "plain/text")
                self.end_headers()

                self.wfile.write(b'All tasks are distributed for reduce\n')
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Endpoint not found.\n")

    def do_POST(self):
        if self.path == "/map":
            content_length = int(self.headers["Content-Length"])
            request_body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(request_body)
            task_id = data['task_id']

            with self.server.locker:
                self.server.map_task_to_status[task_id] = "completed"

        elif self.path == "/reduce":
            content_length = int(self.headers["Content-Length"])
            request_body = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(request_body)
            task_id = data['task_id']

            with self.server.locker:
                self.server.reduce_task_to_status[task_id] = "completed"

        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Endpoint not found.\n")


class MyServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, N=4, M=6):
        self.files = self.get_files()
        self.N, self.M = min(N, len(self.files)), M
        self.path = './inputs'
        self.map_task_to_files = self.distribute_map_tasks()
        self.reduce_task_to_files = self.distribute_reduce_tasks()

        self.manager = multiprocessing.Manager()
        self.current_map_task = self.manager.Value('i', 0)
        self.current_reduce_task = self.manager.Value('i', 0)
        self.map_task_to_status = self.manager.dict(
            {f"map_task_{i}": "incomplete" for i in range(self.N)}
        )
        self.reduce_task_to_status = self.manager.dict(
            {f"reduce_task_{i}": "incomplete" for i in range(self.M)}
        )
        self.locker = self.manager.RLock()

        super().__init__(server_address, RequestHandlerClass)

    def get_files(self):
        return [os.path.join(self.path, file) for file
                in os.listdir(self.path) if file.endswith('.txt')]

    def distribute_map_tasks(self):
        task_to_files = defaultdict(list)
        i = 0
        for j in range(len(self.files)):
            task_to_files[i].append(self.files[j])
            i = (i + 1) % self.N
        return task_to_files

    def distribute_reduce_tasks(self):
        task_to_files = defaultdict(list)
        for reduce_iter in range(self.M):
            files = [f'./outputs_tmp/mp-{map_iter}-{reduce_iter}.txt' for map_iter in range(self.N)]
            task_to_files[reduce_iter] = files
        return task_to_files


if __name__ == '__main__':
    server_address = ("127.0.0.1", 8080)
    parser = argparse.ArgumentParser(
        description="Input number of map tasks - N, and number of reduce tasks - M"
    )

    parser.add_argument("--N", type=int, help="Number of map tasks", required=True)
    parser.add_argument("--M", type=int, help="Number of reduce tasks", required=True)

    args = parser.parse_args()

    N, M = args.N, args.M

    httpd = MyServer(server_address, MyRequestHandler, N=N, M=M)
    print("Server running on", server_address)
    httpd.serve_forever()

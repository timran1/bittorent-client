from p2p import *

assert sys.version_info >= (3, 6)

###############################################################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Peer-to-Peer File System - Server')

    parser.add_argument('--port', help='Port to listen for incoming connections', type=int, required=True)
    args = vars(parser.parse_args())

    port = args['port']
    setup_logging(f"{cur_dir}/logs/p2p-server.log")

    server = Server(port)
    server.start()

    logging.info("Exiting")

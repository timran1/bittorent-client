from p2p import *

assert sys.version_info >= (3, 6)

def testcase_1(peer):
    logging.info(peer.file_list_request())
    # logging.info(peer.file_locations_request("p1-f2.dat"))
    # peer.chunk_register_request("p2-f2.dat", 2)
    peer.download_file("p2-f2.dat")

###############################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Peer-to-Peer File System - Peer')

    parser.add_argument('--data-dir', help='Directory containing all local files', required=True)
    parser.add_argument('--peer-id', help='Unique ID for this peer', required=True, type=int)
    parser.add_argument('--server-ip', help='Server IP', required=True)
    parser.add_argument('--server-port', help='Server port', required=True, type=int)
    args = vars(parser.parse_args())

    peer_id = args['peer_id']

    setup_logging(f"{cur_dir}/logs/p2p-{peer_id}.log")
    logging.info(args)

    # create the single peer object
    peer = Peer(peer_id, args['data_dir'], args['server_ip'], args['server_port'])
    if not peer.connect():
        logging.info ("quitting")
        quit()

    # Run test cases rather than input loop
    testcase = testcase_1
    # testcase = None
    if testcase and peer_id == 1:
        logging.info("Running testcase")
        testcase(peer)
        # logging.info ("quitting")
        # quit()

    # Main interface loop
    while True:
        try:
            print (
                """
Menu:
1. Get available files list
2. Download file
3. Exit
"""
)

            selection = int(input("Make your selection: "))
            if selection == 1:
                files = peer.file_list_request()
                logging.info("*******File List*******")
                for f in files:
                    logging.info(f"{f[0]} ({f[1]} bytes)")
            elif selection == 2:
                file_name = input("Enter file name: ")
                # logging.info (f"Downloading file name={file_name}")
                peer.download_file(file_name)
            elif selection == 3:
                logging.info ("quitting")
                break
            else:
                raise
        except Exception as e:
            logging.warn ("Error parsing input, try again.")
            print(e)

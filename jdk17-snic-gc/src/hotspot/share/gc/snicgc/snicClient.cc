#include "snicClient.hpp"
#include "shareMemSnicClient.hpp"
#include "copyRegionSnicClient.hpp"

int SnicClient::serverSocket;
SnicClient **SnicClient::clients;
int SnicClient::currentHostId;
int SnicClient::alive_clients_num;
std::vector<std::queue<RPCMsg*>> SnicClient::rpcMsgQueues;
struct GlobalPacerData;

SnicClient::SnicClient() {
    // create gc workers.
    uint max_worker_num = (uint)(ConcGCThreads);
    snic_gc_workers = new WorkGang("SNIC concurrent mark", max_worker_num, false, true);
    if (snic_gc_workers == NULL) {
        log_info(gc)("LOG:init error for sni_gc_workers");
        exit(0);
    } else {
        snic_gc_workers->initialize_workers();
    }
    // create task root queues.
    int active_worker_num = (int)(snic_gc_workers->active_workers());
    log_debug(gc)("DGC LOG:init active worker num:%d, total worker num:%u", active_worker_num, snic_gc_workers->total_workers());
    task_queues = new ShenandoahObjToScanQueueSet(active_worker_num);
    for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
        ShenandoahObjToScanQueue *newQueue = new ShenandoahObjToScanQueue();
        newQueue->initialize();
        task_queues->register_queue(i, newQueue);
    }

    if(SnicGCCoorHeuristic){
        char coor_shm_path[1024];
        // strcpy(curSnicShmMemPath, SnicShmMemPath);
        sprintf(coor_shm_path, "%s", SnicGCCoorSHMPath, 0);
        int shm_fd = shm_open(coor_shm_path, O_RDWR, 0666);
        if(shm_fd == -1){
            log_error(gc)("DGC LOG: shm_open failed for coor_shm_path:%s", coor_shm_path);
            perror("shm_open");
            exit(1);
        }
        ftruncate(shm_fd, sizeof(CoorState) * SnicGCCoorClientNum);
        void* shm_ptr = mmap(NULL, sizeof(CoorState) * SnicGCCoorClientNum, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        auto coor_state_array = (CoorState*)shm_ptr;
        coor_state = &coor_state_array[SnicGCCoorClientId];
        log_debug(gc)("DGC LOG: checking magic number, clientId:%d/%d, clientState:%d", SnicGCCoorClientId, coor_state->client_id, coor_state->client_state);
    }
}

int SnicClient::runRPCServer(){
    currentHostId = -1;
    rpcMsgQueues = std::vector<std::queue<RPCMsg*>>(SnicGCHostNum);
    if (UseCompressedClassPointers) {
        java_lang_Class::_oop_size_offset = 32;
    } else {
        java_lang_Class::_oop_size_offset = 36;
    }
    if (UseCompressedOops) {
        InstanceMirrorKlass::_offset_of_static_fields = 112;
    } else {
        InstanceMirrorKlass::_offset_of_static_fields = 184;
    }
    if (UseCompressedClassPointers) {
        java_lang_Class::_static_oop_field_count_offset = 36;
    } else {
        java_lang_Class::_static_oop_field_count_offset = 40;
    }
    int serverPort = RPCPort;
    // 创建 socket
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        log_error(gc)("DGC LOG: Failed to create socket.");
        return -1;
    }

    // SHM mmap is performed lazily by ShareMemSnicClient::handleRPC when the
    // host issues RPC 7 (SHM region registration), so nothing to do here.

    if (!SnicGCShareMemEnabled) {
        clients = new SnicClient*[SnicGCHostNum];
        for (int i = 0; i < SnicGCHostNum; i++) {
            // Pick the DGC-RDMA client implementation by translation flag:
            //   +SnicGCRegionAddressTranslation → CopyRegionSnicClient
            //     (host-VA → local-pool address translation with hot-region
            //     cache eviction; used by fig7 to sweep cache sizes).
            //   -SnicGCRegionAddressTranslation → NoTransSnicCopyRegionClient
            //     (host-VA, no translation, no eviction; default for fig6
            //     hbase, table 3, mix-load).
            if (SnicGCRegionAddressTranslation) {
                clients[i] = new CopyRegionSnicClient();
            } else {
                clients[i] = create_no_trans_snic_copy_region_client();
            }
        }
    } else {
        clients = new SnicClient*[SnicGCHostNum];
        for (int i = 0; i < SnicGCHostNum; i++) {
            clients[i] = new ShareMemSnicClient();
        }
    }

    // if(!SnicGCCoorHeuristic && SnicGCShareMemEnabled && SnicGCGlobalPacer) {
    //     auto globalPacerFileFD = shm_open(SnicShmGlobalPacerPath, O_CREAT | O_RDWR, 0666);
    //     if (globalPacerFileFD < 0) {
    //         log_error(gc)("DGC LOG: create global pacer shm failed, errno:%d, %s, path:%s", errno, strerror(errno), SnicShmGlobalPacerPath);
    //         exit(0);
    //     }

    //     if (ftruncate(globalPacerFileFD, sizeof(GlobalPacerData)) == -1) {
    //         log_error(gc)("DGC LOG: failed to set size of global pacer shm");
    //         exit(0);
    //     }

    //     ShareMemSnicClient::clientGlobalPacerData = (GlobalPacerData*) Universe::hostMmap(NULL, sizeof(GlobalPacerData), 0, globalPacerFileFD, 0);
    //     log_debug(gc)("DGC LOG: mmap global pacer data at %p",ShareMemSnicClient::clientGlobalPacerData);
    //     // only for test:
    //     ShareMemSnicClient::clientGlobalPacerData->round = 6357;
    //     Atomic::store(&(ShareMemSnicClient::clientGlobalPacerData->free), (unsigned long long)(MAX_HOST_NUM));
    // }

    // 绑定 IP 和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(serverPort);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    if (bind(serverSocket, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) < 0) {
        log_error(gc)("Failed to bind to port:%s", strerror(errno));
        close(serverSocket);
        return -1;
    }
    // 监听连接请求
    if (listen(serverSocket, SnicGCHostNum) < 0) {
        log_info(gc)("failed to listen for connection:%s", strerror(errno));
        close(serverSocket);
        return -1;
    }

    sockaddr_in clientAddress{};
    socklen_t clientAddressSize = sizeof(clientAddress);
    log_debug(gc)("DGC LOG: start to accept clientSocket on port %d, serverSocket:%d", serverPort, serverSocket);


    for(int i = 0; i < SnicGCHostNum; i++) {
        clients[i]->clientId = i;
        clients[i]->clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddress, &clientAddressSize);
        if (clients[i]->clientSocket < 0) {
            log_info(gc)("fail to accept client connections:%s", strerror(errno));
            close(serverSocket);
            return -1;
        }
        // OPT#3: disable Nagle on the ack/RPC socket. send_back_int_ack sends
        // 8-byte size_t values one-by-one from both the main RPC handler and the
        // wait_part_copy_finish_work background thread. Without TCP_NODELAY,
        // each small send after the first gets held by Nagle until either the
        // host ACKs (delayed ACK = 40ms on Linux) or more data queues up. The
        // net effect is ~40ms per small-send burst, which dominated the 85ms
        // floor observed in per-cycle rdmaMark measurements for small benchmarks
        // (luindex baseline marking ~1.4ms, rdma4 ~88ms).
        int tcp_nodelay = 1;
        if (setsockopt(clients[i]->clientSocket, IPPROTO_TCP, TCP_NODELAY,
                       &tcp_nodelay, sizeof(tcp_nodelay)) < 0) {
            log_debug(gc)("DGC LOG: setsockopt TCP_NODELAY failed on clientSocket: %s", strerror(errno));
        } else {
            log_debug(gc)("DGC LOG: TCP_NODELAY enabled on clientSocket fd=%d", clients[i]->clientSocket);
        }
        log_info(gc)("Server is listening on port %d, accept clientSocket:%d, serverSocket:%d", serverPort, clients[i]->clientSocket, serverSocket);
    }
    log_debug(gc)("DGC LOG: get all clientSocket");
    alive_clients_num = SnicGCHostNum;
    for(int i = 0; i < SnicGCHostNum; i++) {
        clients[i]->send_back_ack(i);
    }
    char *buffer = new char[MAX_RPC_BUFFER_SIZE];
    bool isInCriticalSection = false;
    while (true) {
        if(currentHostId == -1) {
            // do not modify global pacer when some host is marking
            // if(!SnicGCCoorHeuristic && SnicGCGlobalPacer && SnicGCShareMemEnabled){
            //     ShareMemSnicClient::schedule_global_pacer();
            //     if(ShareMemSnicClient::schedule_global_pacer_decided){
            //         // log_debug(gc)("DGC LOG: schedule_global_pacer func decided currentHostId = %d", ShareMemSnicClient::schedule_host_ids.front());
            //         currentHostId = ShareMemSnicClient::schedule_host_ids.front();
            //     }
            // }
            if (SnicGCShareMemEnabled && SnicGCCoorHeuristic) {
                // clients[0]->waitCoordinatorUpdateFinished();
                // clients[0]->updateClientState(ClientStates::SCHEDULE_GLOBAL_PACER);
            }
            if (currentHostId == -1) {
                // only check all rpc queues when currentHostId is stll -1 after the global pacer scheduling
                for(int i = 0; i < SnicGCHostNum; i++) {
                    if(rpcMsgQueues[i].size() > 0) {
                        currentHostId = i;
                        break;
                    }
                }
            }
        }
        // for (int i = 0; i < SnicGCHostNum; i++) {
        //     log_debug(gc)("DGC LOG: rpcMsgQueues[%d].size() = %d", i, rpcMsgQueues[i].size());
        // }
        // log_debug(gc)("DGC LOG: currentHostId = %d", currentHostId);
        // os::naked_short_sleep(10);
        if(currentHostId != -1) {
            if(rpcMsgQueues[currentHostId].size() > 0) {
                RPCMsg *msg = rpcMsgQueues[currentHostId].front();
                rpcMsgQueues[currentHostId].pop();
                clients[currentHostId]->handleRPC(msg->rpcType, msg->hostId, msg->sz, msg->payload);
                bool should_start_critical_section = (SnicGCShareMemEnabled && msg->rpcType == 5) || (!SnicGCShareMemEnabled && msg->rpcType == 8);
                bool should_finish_critical_section = (SnicGCShareMemEnabled && msg->rpcType == 9) || (!SnicGCShareMemEnabled && msg->rpcType == 10);
                if (should_start_critical_section) {
                    isInCriticalSection = true;
                }
                if (should_finish_critical_section) {
                    isInCriticalSection = false;
                    if (!SnicGCCoorHeuristic && !SnicGCShareMemEnabled && SnicGCHostNum > 1) {
                        auto newHostId = (currentHostId + 1) % SnicGCHostNum;
                        log_debug(gc)("DGC LOG: try to force host %d to gc", newHostId);
                        // Virtual dispatch — both CopyRegionSnicClient
                        // and NoTransSnicCopyRegionClient override
                        // forceGCByDPUClient(); base SnicClient is a
                        // no-op so the SHM-only branch above is
                        // covered too.
                        clients[newHostId]->forceGCByDPUClient();
                    }
                }
                if(rpcMsgQueues[currentHostId].size() == 0 && !isInCriticalSection) {
                    currentHostId = -1;
                }
                // delete msg;
            }else{
                // currentHostId = -1;
                resolveRPC(buffer);
            }
        }
        else{
            resolveRPC(buffer);
        }
    }
    // delete (buffer);
}

void SnicClient::resolveRPC(void* buffer) {
    fd_set readfds;
    int maxSocket = -1;
    FD_ZERO(&readfds); // 清空集合
    std::vector<int> sockets;
    for(int i = 0; i < SnicGCHostNum; i++) {
        sockets.push_back(clients[i]->clientSocket);
        if (SnicGCHostNum > 1) {
            // only use non-block sockets when this client is serving multiple hosts.
            fcntl(sockets[i], F_SETFL, O_NONBLOCK);
        }
        FD_SET(sockets[i], &readfds);
        maxSocket = std::max(maxSocket, sockets[i]);
    }
    int respondingHostId = -1;
    if (SnicGCHostNum > 1) {
        // only use non-block sockets when this client is serving multiple hosts.
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 1000;
        int activity = select(maxSocket + 1, &readfds, nullptr, nullptr, &timeout);
        if (activity < 0) {
            log_error(gc)("DGC LOG: select error");
            exit(0);
        }
        if (activity == 0) {
            // log_error(gc)("DGC LOG: select timeout");
            return;
        }
        for(int i = 0; i < SnicGCHostNum; i++) {
            if (FD_ISSET(sockets[i], &readfds)){
                respondingHostId = i;
                break;
            }
        }
        if(respondingHostId == -1) {
            log_error(gc)("DGC LOG: no socket is ready");
            return;
        }
    } else {
        // copyRegionSnicClient only supports for 1 host for now.
        respondingHostId = 0;
    }
    int rpcType;
    int hostId;
    int sz;
    ssize_t recv_len = 0;
    recv_len = recv(sockets[respondingHostId], buffer, MAX_RPC_BUFFER_SIZE, 0);
    while (recv_len <= 0) {
        recv_len = recv(sockets[respondingHostId], buffer, MAX_RPC_BUFFER_SIZE, 0);
        if (recv_len < 0) {
            log_warning(gc)("DGC LOG: fail to get new package 1, %d, %s", errno, strerror(errno));
        }
    }
    rpcType = ((short *)buffer)[0];
    hostId = ((short *)buffer)[1];
    sz = ((int *)buffer)[1];
    while (recv_len > sz) {
        recv_len -= sz;
        sz -= 2 * sizeof(int);
        buffer = ((int *)buffer) + 2;
        if(hostId != respondingHostId) {
            log_error(gc)("DGC LOG: hostId is not respondingHostId");
            exit(0);
        }
        rpcMsgQueues[hostId].push(new RPCMsg(rpcType, hostId, sz, buffer));
        buffer = (void *)((char *)(buffer) + sz);
        rpcType = ((short *)buffer)[0];
        hostId = ((short *)buffer)[1];
        sz = ((int *)buffer)[1];
    }
    if (sz > MAX_RPC_BUFFER_SIZE) {
        log_error(gc)("DGC LOG: message size larger than buffer size 1.");
        exit(0);
    }
    while (recv_len < sz) {
        // log_dev_debug(gc)("DGC LOG: waiting for rest of buffer, size=%d", sz - recv_len);
        ssize_t cur_recv_len = recv(sockets[respondingHostId], (void *)((char *)buffer + recv_len), sz - recv_len, 0);
        if (cur_recv_len > 0) {
            recv_len += cur_recv_len;
        } else {
            if (cur_recv_len < 0) {
                log_error(gc)("DGC LOG: fail to get new package 2, %d, %s", errno, strerror(errno));
                // exit(0);
            }
        }
    }
    sz -= 2 * sizeof(int);
    buffer = ((int *)buffer) + 2;
    if(hostId != respondingHostId) {
        log_error(gc)("DGC LOG: hostId is not respondingHostId");
        exit(0);
    }
    RPCMsg *msg = new RPCMsg(rpcType, hostId, sz, buffer);
    rpcMsgQueues[hostId].push(msg);
}

void SnicClient::send_back_ack(char msg) {
    char ack = (char) msg;
    if (send(clientSocket, (char *)(&ack), sizeof(char), 0) == -1) {
        log_error(gc)("DGC LOG: snicClient send ack to host failed");
        exit(0);
    }
}

void SnicClient::handleRPC(int rpcType, int hostId, int bufferSize, void* payload) {
    log_info(gc)("Handling RPC of type %d", rpcType);
}

void SnicClient::send_back_int_ack(size_t ack) {
    size_t notify = ack;
    if (send(clientSocket, (size_t *)(&notify), sizeof(size_t), 0) <= 0) {
        log_error(gc)("DGC LOG: send back int ack failed:errno %d, %s", errno, strerror(errno));
        exit(0);
    }
}

void SnicClient::updateClientState(int clientState) {
    if(SnicGCCoorHeuristic){
        if (coor_state->client_state != ClientStates::STATE_UPDATE_FINISHED && coor_state->client_state != ClientStates::INIT_CLIENT_STATE) {
            log_debug(gc)("DGC LOG: client %d is not ready to update state to %d, current state: %d", coor_state->client_id, clientState, coor_state->client_state);
            return;
        }
        if (clientState != ClientStates::UPDATE_GLOBAL_PACER_LIVENESS) {
            // log_debug(gc)("DGC LOG: client %d update state from %d to %d", coor_state->client_id, coor_state->client_state, clientState);
        }
        coor_state->client_state = clientState;
    }
    waitCoordinatorUpdateFinished();
}

void SnicClient::waitCoordinatorUpdateFinished() {
    while (coor_state->client_state != ClientStates::STATE_UPDATE_FINISHED && coor_state->client_state != ClientStates::INIT_CLIENT_STATE) {}
}

void SnicClient::set_marked_liveness(unsigned long long liveness) {
    if(SnicGCCoorHeuristic){
        coor_state->marked_liveness = liveness;
    }
}

void SnicClient::add_marked_liveness(unsigned long long liveness) {
    if(SnicGCCoorHeuristic){
        Atomic::add(&(coor_state->marked_liveness), (long long)liveness);
    }
}

unsigned long long SnicClient::get_avg_mark_time() {
    if (historyMarkTime.size() == 0) {
        if (SnicGCShareMemEnabled) {
            return 300;
        } else {
            return 500;
        }
    }
    unsigned long long sum = 0;
    for (int i = historyMarkTime.size() - 1; i >= 0 && i >= (int)historyMarkTime.size() - 10; i--) {
        sum += historyMarkTime[i];
    }
    return sum / (historyMarkTime.size() > 10 ? 10 : historyMarkTime.size());
}

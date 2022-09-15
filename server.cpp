#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

/////////////////////////////
#include <pthread.h>
#include <iostream>
#include <semaphore.h>
#include <assert.h>
#include <queue>
#include <vector>
#include <tuple>
#include <string>
#include <bits/stdc++.h>
using namespace std;

#define THREADPOOL_SIZE 200 //program made on the assumption that n, ie the server thread number is < 200
#define DICTSIZE 101
#define PORTNO 8001
#define SERVER_BACKLOG 100
typedef struct sockaddr sad;
typedef struct sockaddr_in sadin;
typedef long long int LL;

const LL buff_sz = 1048576;
int num_serverthreads;
string dictionary[DICTSIZE];
pthread_t serverthreads[THREADPOOL_SIZE];
queue<int*> clirequests;
// string dictionary[100];
pthread_mutex_t queuelock;
pthread_cond_t available;
sem_t qavailable;
pthread_mutex_t dictlock[DICTSIZE];

pair<string, int> read_string_from_socket(const int &fd, int bytes)
{
    string output;
    output.resize(bytes);

    int bytes_received = read(fd, &output[0], bytes - 1);
    printf("%d bytes received from client\n",bytes_received);
    if (bytes_received <= 0)
    {
        cerr << "Failed to read data from socket. \n";
    }

    output[bytes_received] = 0;
    output.resize(bytes_received);
    return {output, bytes_received};
}

////////////////////////////////////////////////////////////////////////////
//The below part contains functions that manipulate the dictionary to do the
// operations as requested by the client    
string dictinsert(int key, const string &s)
{
    string retstr;
    string passedstr=s;
    pthread_mutex_lock(&dictlock[key]);
    if(dictionary[key]=="bleh")
    {
        dictionary[key]=passedstr;
        retstr="Insertion succesful: ";
        retstr+=passedstr;
        retstr+=" inserted at pos ";
        retstr+=to_string(key);
    }
    else
    retstr="Key Already exists, try update command";
    pthread_mutex_unlock(&dictlock[key]);
    return retstr;
}

string dictdelete(int key)
{
    string retstr;
    pthread_mutex_lock(&dictlock[key]);
    if(dictionary[key]=="bleh")
    {
        retstr="No such key exist: ";
        retstr+=to_string(key);
    }
    else
    {
        dictionary[key]="bleh";
        retstr="Deletion successful for key: ";
        retstr+=to_string(key);
    }
    pthread_mutex_unlock(&dictlock[key]);
    return retstr;
}

string dictupdate(int key, const string &s)
{
    string retstr;
    string passedstr=s;
    pthread_mutex_lock(&dictlock[key]);
    if(dictionary[key]=="bleh")
    {
        retstr="No such key exist: ";
        retstr+=to_string(key);
    }
    else
    {
        dictionary[key]=passedstr;
        retstr="Updated ";
        retstr+=to_string(key);
        retstr+=" as ";
        retstr+=passedstr;
    }
    pthread_mutex_unlock(&dictlock[key]);
    return retstr;
}

string dictconcat(int key1, int key2)
{
    string retstr;
    // string passedstr=s;
    pthread_mutex_lock(&dictlock[key1]);
    pthread_mutex_lock(&dictlock[key2]);
    if(dictionary[key1]=="bleh" || dictionary[key2]=="bleh")
    {
        retstr="Concat failed as at least one of the keys does not exist";
    }
    else
    {
        string str1=dictionary[key1];
        string str2=dictionary[key2];
        dictionary[key1]=str1+str2;
        dictionary[key2]=str2+str1;
        retstr=dictionary[key2];
    }
    pthread_mutex_unlock(&dictlock[key1]);
    pthread_mutex_unlock(&dictlock[key2]);
    return retstr;
}

string dictfetch(int key)
{
    string retstr;
    pthread_mutex_lock(&dictlock[key]);
    if(dictionary[key]=="bleh")
    {
        retstr="No such key exist: ";
        retstr+=to_string(key);
    }
    else
    {
        retstr=dictionary[key];
    }
    pthread_mutex_unlock(&dictlock[key]);
    return retstr;
}

////////////////////////////////////////////////////////////////////////////

// This is th efuntion that does the stuff that client has requested
string dostuff(const string &s)
{
    int pos;
    string retstring;
    string arrived=s;
    string command;
    pos=arrived.find(" ");
    command=arrived.substr(0,pos);
    string rem=arrived.substr(pos+1);
    arrived=rem;
    if(command=="insert")
    {
        pos=arrived.find(" ");
        string keystr=arrived.substr(0,pos);
        rem=arrived.substr(pos+1);
        string value=rem;
        int key=stoi(keystr);
        retstring=dictinsert(key,value);
    }
    else if(command=="delete")
    {
        pos=arrived.find(" ");
        string keystr=arrived.substr(0,pos);
        // rem=arrived.substr(pos+1);
        // string value=rem;
        int key=stoi(keystr);
        retstring=dictdelete(key);
    }
    if(command=="update")
    {
        pos=arrived.find(" ");
        string keystr=arrived.substr(0,pos);
        rem=arrived.substr(pos+1);
        string value=rem;
        int key=stoi(keystr);
        retstring=dictupdate(key,value);
    }
    if(command=="concat")
    {
        pos=arrived.find(" ");
        string key1str=arrived.substr(0,pos);
        rem=arrived.substr(pos+1);
        string key2str=rem;
        int key1=stoi(key1str);
        int key2=stoi(key2str);
        retstring=dictconcat(key1,key2);
    }
    if(command=="fetch")
    {
        // pos=arrived.find(" ");
        // string keystr=arrived.substr(0,pos);
        string keystr=arrived;
        // rem=arrived.substr(pos+1);
        // string value=rem;
        int key=stoi(keystr);
        retstring=dictfetch(key);
    }
    return retstring;
}

int send_string_on_socket(int fd, const string &s)
{
    // debug(s.length());
    int bytes_sent = write(fd, s.c_str(), s.length());
    if (bytes_sent < 0)
    {
        cerr << "Failed to SEND DATA via socket.\n";
    }

    return bytes_sent;
}

// Handles connection from client
void connectionhandler(int clientfd)
{
    int received_num, sent_num;

    /* read message from client */
    int ret_val = 1;
    string cmd;
    tie(cmd, received_num) = read_string_from_socket(clientfd, buff_sz);
    ret_val = received_num;
    if(ret_val<=0)
    {
        cout<<"Server Couldnt read message from cient, closing connetion"<<endl;
        close(clientfd);
        cout<<"Disconnected from client"<<endl;
        return;
    }
    cout << "Client sent : " << cmd << endl;
    string retmsg=dostuff(cmd);
    int sent_to_client = send_string_on_socket(clientfd, retmsg);
    if(sent_to_client==-1)
    {
        cout<<"Error while sending to client"<<endl;
        close(clientfd);
        cout<<"Disconnected from client"<<endl;
        return;
    }
    cout<<"sent response to: "<<cmd<<endl;
}

void* threadfunction(void* args)
{
    while(1)
    {
        sem_wait(&qavailable);
        pthread_mutex_lock(&queuelock);
        // cout<<"thread has taken lock"<<endl;
        // pthread_cond_wait(&available,&queuelock);
        // cout<<"condition satisfies"<<endl;
        int* pclient=clirequests.front();
        if(pclient!=NULL)
        {
            clirequests.pop();
        }
        // clirequests.pop();
        pthread_mutex_unlock(&queuelock);
        // cout<<"thread has given lock"<<endl;
        if(pclient!=NULL)
        {
            connectionhandler(*pclient);
        }
        // pthread_mutex_unlock(&queuelock);
    }
}

int main(int argc, char* argv[])
{
    sem_init(&qavailable,0,0);
    if(argc!=2)
    {
        printf("invalid usage\n format: ./server <number of threads in pool>\n");
        exit(0);
    }
    pthread_mutex_init(&queuelock,NULL);
    pthread_cond_init(&available,NULL);
    for(int i=0;i<DICTSIZE;i++)
    {
        dictionary[i]="bleh";
        pthread_mutex_init(&dictlock[i],NULL);
    }
    num_serverthreads=atoi(argv[1]);
    printf("Server started with %d threads in pool\n",num_serverthreads);
    for(int i=0;i<num_serverthreads;i++)
    {
        pthread_create(&serverthreads[i],NULL,threadfunction,NULL);
    }
    int server_socket, client_socket, addr_size;
    sadin serveraddr,clientaddr;
    socklen_t clientlength;
    server_socket=socket(AF_INET,SOCK_STREAM,0);
    bzero((char*)&serveraddr,sizeof(serveraddr));
    serveraddr.sin_addr.s_addr=INADDR_ANY;
    serveraddr.sin_family=AF_INET;
    serveraddr.sin_port=htons(PORTNO);

    if(bind(server_socket,(sad*)&serveraddr,sizeof(serveraddr))<0)
    {
        cout<<"bind failed"<<endl;
    }
    listen(server_socket,SERVER_BACKLOG);
    // clientlength=sizeof(clientaddr);

    while(1)
    {
        clientlength=sizeof(clientaddr);
        printf("Waiting for new client to request for a connection\n");
        client_socket=accept(server_socket,(sad*)&clientaddr,&clientlength);
        // printf("got a request :) \n");
        int* pclient=(int*)malloc(sizeof(int));
        *pclient=client_socket;
        pthread_mutex_lock(&queuelock);
        // cout<<"qlock obtained"<<endl;
        clirequests.push(pclient);
        sem_post(&qavailable);  // signals that a client is waiting to be served in queue
        // pthread_cond_signal(&available);
        pthread_mutex_unlock(&queuelock);
        // pthread_cond_signal(&available);
        // pthread_mutex_unlock(&queuelock);
        // break;
    }
    return 0;
}

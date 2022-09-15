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

#define PORTNO 8001
#define MAX_SIZE 100    //assuming that m, ie the number of client requests is < 100
typedef struct sockaddr sad;
typedef struct sockaddr_in sadin;
typedef long long int LL;
pthread_mutex_t writelock=PTHREAD_MUTEX_INITIALIZER;    //simple mutex to preven multiple threads from
//concurrently writing to the terminal

const LL buff_sz = 1048576;

// Structure to store the client requests to be sent
struct Request{
    int time;
    // char command[MAX_SIZE];
    string command;
    int key1;
    int key2;
    // char value[MAX_SIZE];
    string value;
};
typedef struct Request* request;


int socket_fd[MAX_SIZE];
int num_requests;
request reqarray[MAX_SIZE]; //array of client requests
pthread_t clientthreads[MAX_SIZE];
// int threadid[MAX_SIZE];

int get_socket_fd(sadin *ptr);


//gets the string to be sent to the server to get the response
string getstring(request curreq)
{
    string str = curreq->command;
    if(str=="insert")
    {
        str+=" ";
        str+=to_string(curreq->key1);
        str+=" ";
        str+=curreq->value;
    }
    else if(str=="delete")
    {
        str+=" ";
        str+=to_string(curreq->key1);
    }
    else if(str=="update")
    {
        str+=" ";
        str+=to_string(curreq->key1);
        str+=" ";
        str+=curreq->value;
    }
    else if(str=="concat")
    {
        str+=" ";
        str+=to_string(curreq->key1);
        str+=" ";
        str+=to_string(curreq->key2);
    }
    else if(str=="fetch")
    {
        str+=" ";
        str+=to_string(curreq->key1);
    }

    return str;
}

//function to send string to socket of the server
int sendtosock(int fd, const string &s)
{
    
    int bytes_sent = write(fd, s.c_str(), s.length());

    if (bytes_sent < 0)
    {
        cerr << "Failed to SEND DATA on socket.\n";
        // return 
        exit(-1);
    }
    return bytes_sent;
}

// Reads the incoming message from server
pair<string,int> readfromsocket(int fd, int bytes)
{
    string output;
    output.resize(bytes);

    int bytes_received = read(fd, &output[0], bytes - 1);

    if (bytes_received <= 0)
    {
        cerr << "Failed to read data from socket. Seems server has closed socket\n";
        // return 
        exit(-1);
    }

    output[bytes_received] = 0;
    output.resize(bytes_received);

    return {output, bytes_received};
}

//Handler function for client threads
void* reqhandler(void* args)
{
    int reqno=*((int*)args);
    request curreq=reqarray[reqno];
    int sleeptime=curreq->time;
    if(sleeptime>1)
    {
        sleep(sleeptime-1);
    }
    sadin server;
    socket_fd[reqno]=get_socket_fd(&server);
    // cout<<"Connection to server successful" << endl;
    // cout<<"Thread for request: "<<reqno<<"running"<<endl;
    string to_send=getstring(curreq);
    // cout<<"Sending to server: "<<to_send<<endl;
    // cout<<"gonna try sending req\n";
    sendtosock(socket_fd[reqno],to_send);
    // cout<<"req sent\n";
    int readbytes;
    string outputstr;
    tie(outputstr,readbytes)=readfromsocket(socket_fd[reqno],buff_sz);
    pthread_mutex_lock(&writelock);
    cout<<reqno<<":"<<pthread_self()<<":"<<outputstr<<endl;
    pthread_mutex_unlock(&writelock);
    return NULL;
}

int get_socket_fd(sadin *ptr)
{
    sadin server_obj = *ptr;
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0)
    {
        perror("Error in socket creation for CLIENT");
        exit(-1);
    }
    int port_num = PORTNO;

    memset(&server_obj, 0, sizeof(server_obj)); 
    server_obj.sin_family = AF_INET;
    server_obj.sin_port = htons(port_num); 

    if (connect(socket_fd, (sad *)&server_obj, sizeof(server_obj)) < 0)
    {
        perror("Problem in connecting to the server");
        exit(-1);
    }

    return socket_fd;
}

int main()
{
    int rtime, rkey;
    cin>>num_requests;
    int handlernum[num_requests];
    // pthread_t threads[num_requests];
    for(int i=0;i<num_requests;i++)
    {
        int time, key1, key2;
        string cmd,val;
        reqarray[i]=(request)malloc(sizeof(struct Request));
        cin>>time>>cmd;
        reqarray[i]->time=time;
        reqarray[i]->command=cmd;
        if(reqarray[i]->command=="insert")
        {
            cin>>key1>>val;
            reqarray[i]->key1=key1;
            reqarray[i]->value=val;
            reqarray[i]->key2=-1;
        }
        else if(reqarray[i]->command=="delete")
        {
            cin>>key1;
            reqarray[i]->key1=key1;
            reqarray[i]->value="onnulla";
            reqarray[i]->key2=-1;
        }
        else if(reqarray[i]->command=="update")
        {
            cin>>key1>>val;
            reqarray[i]->key1=key1;
            reqarray[i]->value=val;
            reqarray[i]->key2=-1;
        }
        else if(reqarray[i]->command=="concat")
        {
            cin>>key1>>key2;
            reqarray[i]->key1=key1;
            reqarray[i]->key2=key2;
            reqarray[i]->value="onnulla";
        }
        else if(reqarray[i]->command=="fetch")
        {
            fflush(stdin);
            cin>>key1;
            reqarray[i]->key1=key1;
            reqarray[i]->value="onnulla";
            reqarray[i]->key2=-1;
            // cout<<"Entered fetch "<<reqarray[i]->key1<<" ("<<to_string(reqarray[i]->key1)<<") "<<endl;
        }
        else
        {
            cout<<"invalid option"<<endl;
        }
    }
    // sadin server;
    // socket_fd=get_socket_fd(&server);
    // cout<<"Connection to server successful" << endl;
    for(int i=0;i<num_requests;i++)
    {
        // sadin server;
        // socket_fd=get_socket_fd(&server);
        // cout<<"Connection to server successful" << endl;
        handlernum[i]=i;
        pthread_create(&clientthreads[i],NULL,reqhandler,(void*)&handlernum[i]);
        // threadid[i]=clientthreads[i];
    }
    for(int i=0;i<num_requests;i++)
    {
        pthread_join(clientthreads[i],NULL);
    }
}
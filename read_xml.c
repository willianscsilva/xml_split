/**
 * Nome: read_xml
 * Objetivo: Ler um xml e dividi-lo em arquivos menores, seguindo regras definidas anteriormente, em outro programa.
 * Autor: Will
 * Data Criação: 14/10/2013 
 **/
/**
 * Para compilar use:
 * gcc read_xml.c -lcurl -lpcre -pthread -o read_xml
 **/
#include <netinet/in.h>
#include <curl/curl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <pcre.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <syslog.h>
#include <netdb.h>

#define BUFFER_SIZE (512 * 1024 * 1024)
 
int result, exists;
int preg_match(char* expression, char* str, int match);
int file_exists (char * fileName);
char *msg_log;

char* substring(const char* str, size_t begin, size_t len);
char* open_url(char* url);
char* read_file(const char* path_file);
//char* get_block_struct(char* xml_pointer, size_t inicio, size_t fim, size_t iterat);
void *get_block_struct(void *parm);
void write_file(const char* path_file, char* content);
void write_log(char* log_type, char* msg);
void daemonize();
void gen_file();
void error_socket(const char *msg);
int read_socket(int argc, char **argv);
void write_in_socket(int argc, char **argv);

static size_t curl_write( void *ptr, size_t size, size_t nmemb, void *stream);

struct write_result 
{
	char *data;
	int pos;
};

struct xml_conf
{
	char* url;
	char* id_login;
	char* total_sub_arquivos;	
};

struct match_pos
{
	size_t begin, end;	
	char* content;
};

struct block_content
{
	char* content;
};

typedef struct
{
	char* xml_pointer; 
	char* id_login;
	char* url;
	size_t inicio; 
	size_t fim; 
	size_t iterat;	
} thread_parm_t;

struct match_pos res;
struct xml_conf xml_c;
struct block_content b_content;
/**
 * Funcao callback para escrever o buffer do curl no nosso buffer
 **/
static size_t curl_write( void *ptr, size_t size, size_t nmemb, void *stream) 
{

	struct write_result *result = (struct write_result *)stream;
	/* Will we overflow on this write? */
	if(result->pos + size * nmemb >= BUFFER_SIZE - 1) 
	{
		fprintf(stderr, "curl error: too small buffer\n");
		return 0;
	}

	/* Copy curl's stream buffer into our own buffer */
	memcpy(result->data + result->pos, ptr, size * nmemb);

	/* Advance the position */
	result->pos += size * nmemb;

	return size * nmemb;
}
/** 
 * Retorna uma parte de uma string. 
 **/
char* substring(const char* str, size_t begin, size_t len)
{
  	if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < (begin+len))
		return 0;

  	return strndup(str + begin, len);
}
/**
 * Executar a correspondência de uma expressão regular. 
 **/
int preg_match(char* expression, char* str, int match)
{
	pcre *re;
	const char *error;
	int options = 0;
	int erroffset;
	int ovector[30];
	int rc;

	re = pcre_compile(expression, options, &error, &erroffset, NULL);
	if(re == NULL) 
	{
		printf("Could not compile re.\n");		
	}
	else
	{	
		rc = pcre_exec(re, NULL, str, strlen(str), 0, 0, ovector, 30);
		if(rc < 0)
		{
			pcre_free(re);
		}
		else
		{
			pcre_free(re);
			if(match == 1)
			{
				int begin;
				int end;
				int i;			
				for (i = 0; i < rc; i++)
				{
					begin = ovector[2 * i];
					end = ovector[2 * i + 1] - ovector[2 * i];
				}			
				res.begin = begin;
				res.end = end;
				res.content = strndup(str + begin, end);
			}
			return 1;
		}
	}
	return 0;
}

char *open_url(char* url)
{
	CURL *curl = curl_easy_init();
	char *data;
	
	data = malloc(BUFFER_SIZE);
	if (!data)
		fprintf(stderr, "Error allocating %d bytes.\n", BUFFER_SIZE);

	struct write_result write_result = {
		.data = data,
		.pos = 0
	};
	
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &write_result);

	curl_easy_perform(curl);
	
	data[write_result.pos] = '\0';
	
	curl_easy_cleanup(curl);
	curl_global_cleanup();
	return data;
}
/**
 * Funcao recursiva, para separam em blocos de x de tamanho
 * isso permite separar o xml em arquivos menores.
 **/ 
//char *get_block_struct(char* xml_pointer, size_t inicio, size_t fim, size_t iterat)
void *get_block_struct(void *param)
{
	thread_parm_t *p = (thread_parm_t *)param;	
	char* xml_pointer = p->xml_pointer;
	char* end_block;
	char* content = malloc(BUFFER_SIZE*sizeof(char));
	msg_log = NULL;
	msg_log = malloc(BUFFER_SIZE);
		
	size_t inicio = p->inicio; 
	size_t fim = p->fim; 
	size_t iterat = p->iterat;
	
	size_t last_chars = 0;
	size_t n_last_chars = 10;
	size_t iterat_interno = 0;
	
	if(iterat != 0)
	{	
		iterat_interno = iterat;
	}
	if(iterat_interno != 0)
	{
		iterat_interno = iterat_interno+1;		
		content = substring((const char*) xml_pointer, inicio, fim+iterat_interno);		
		last_chars = fim-n_last_chars;
		end_block = substring((const char*) content, last_chars+iterat_interno, n_last_chars);
	}
	else
	{			
		content = substring((const char*) xml_pointer, inicio, fim);
		last_chars = fim-n_last_chars;
		end_block = substring((const char*) content, last_chars, n_last_chars);
	}
	/* end_block == NULL, indica fim inesperado do xml. */
	if(end_block == NULL)
	{
		printf("UM ERRO NO XML ACONTECEU: END_BLOCK => %s\n", end_block);
		strcpy(msg_log, "ERRO XML: ");
		strcat(msg_log, "URL: ");
		strcat(msg_log, p->url);
		strcat(msg_log, " ID_LOGIN: ");
		strcat(msg_log, p->id_login);
		strcat(msg_log, "\n");
		write_log("LOG_ERR", msg_log);
		free(msg_log);
	}
	else
	{
		result = preg_match("(/item>|/rss>)", end_block, 0);	
		if(result == 0)
		{
			if(iterat_interno > 1)
			{
				free(content);
				free(end_block);
				//return get_block_struct(xml_pointer, inicio, fim, iterat_interno);			
				b_content.content = "";			
				p->inicio = inicio; 
				p->fim = fim; 
				p->iterat = iterat_interno;			
				get_block_struct((void *) p);
			}
			else
			{
				free(content);
				free(end_block);
				//return get_block_struct(xml_pointer, inicio, fim, 1);
				b_content.content = "";
				p->inicio = inicio;
				p->fim = fim;
				p->iterat = 1;			
				get_block_struct((void *) p);
			}
		}
		else
		{
			b_content.content = content;
			//return content;
		}
	}
}
/**
 * Verifica se um arquivo ou diretorio existe. 
 **/ 
int file_exists (char * fileName)
{
   struct stat buf;
   int i = stat ( fileName, &buf );
     /* File found */
     if ( i == 0 )
     {
       return 1;
     }
     return 0;
       
}
/**
 * Le o arquivo, e a saida, coloca em um buffer.
 **/ 
char* read_file(const char* path_file)
{
	char * buffer = 0;
	long length;
	FILE * fp = fopen (path_file, "rb");
	
	if (fp != NULL)
	{
		fseek (fp, 0, SEEK_END);
		length = ftell (fp);
		fseek (fp, 0, SEEK_SET);
		buffer = malloc (length);
		if (buffer)
		{
			fread (buffer, 1, length, fp);
		}
		fclose (fp);
	}
	return buffer;
}
/**
 * Escreve o buffer em um arquivo.
 **/ 
void write_file(const char* path_file, char* content)
{
	size_t size_file = strlen(content);
	FILE * fp;
	exists = file_exists((char *)path_file);
	if(exists == 0)
	{
		fp = fopen(path_file, "w+");
	}
	else
	{
		fp = fopen(path_file, "a+");
	}
	if (fp != NULL)
	{
		fwrite(content, 1, size_file , fp);
	}
	fclose(fp);
}
/**
 * Escreve uma mensagem em um arquivo.
 **/
void write_log(char* log_type, char* msg)
{
	char* path = "/tmp/xml/log/";	
	exists = file_exists(path);
	if(exists == 0)
	{
		mkdir(path, S_IRWXU|S_IRGRP|S_IXGRP);
	}
	FILE * fp;
	fp = fopen("/tmp/xml/log/log_xml.log","a+");
	if (fp != NULL)
  	{
		if(strcmp(log_type, "LOG_NOTICE"))
		{
			fputs(msg, fp);	
		}
		else if(strcmp(log_type, "LOG_ERR"))
		{
			fputs(msg, fp);
		}
	}
	fclose(fp);
}
/**
 * Cria um fork do processo, assim gerando um processo filho.
 **/  
void daemonize()
{
	pid_t pid, sid;
	/* Clone ourselves to make a child */
	pid = fork();
	/* If the pid is less than zero,
	 * something went wrong when forking 
	 * */
	if (pid < 0) 
	{
		exit(EXIT_FAILURE);
	}
	/*	
	 * If the pid we got back was greater
	 * than zero, then the clone was
	 * successful and we are the parent. 
	 * */
	if (pid > 0) 
	{
		exit(EXIT_SUCCESS);
	}
	/* If execution reaches this point we are the child */
	/* Set the umask to zero */
	umask(0);
	
	/* Sends a message to the syslog daemon */	
	write_log("LOG_NOTICE", "Successfully started daemon\n");

	/* Try to create our own process group */
	sid = setsid();
	if (sid < 0) 
	{
		write_log("LOG_ERR", "Could not create process group\n");
		exit(EXIT_FAILURE);
	}
	/* Change the current working directory */
	if ((chdir("/")) < 0) 
	{
		write_log("LOG_ERR", "Could not change working directory to /\n");
		exit(EXIT_FAILURE);
	}
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);	
}
/**
 * Parseia o arquivo gerado pela rotina em python,
 * esse arquivo, contem alguns dados sobre o xml e o cliente.
 **/ 
void parse_xml_conf()
{
	const char path_file[] = "/tmp/xml/xml_conf.txt";
	FILE * fp;
	char * line = NULL;
	size_t len = 0;
	ssize_t read;
	pthread_t pth;

	fp = fopen(path_file, "r");
	if (fp != NULL)
	{
		while ((read = getline(&line, &len, fp)) != -1) 
		{
			result = preg_match("(url: (.*?)[ |]+)", line, 1);
			xml_c.url = res.content;
		   
			preg_match("(id_login: ([0-9]+)[ |]+)", line, 1);
			xml_c.id_login = res.content;
		   
			preg_match("(total_sub_arquivos: ([0-9]+))", line, 1);
			xml_c.total_sub_arquivos = res.content;
			if(result == 1)
			{
				//gen_file();
				pthread_create(&pth, NULL, gen_file, NULL);
				pthread_join(pth, NULL);
			}
		}
		if (line)
		   free(line);
	}
}
/**
 * Gera os subarquivos.
 **/
void gen_file()
{
	int i;
	char char_i[10];
	char underline[1];
	strcpy(underline,"_");
	char path_file[50];
	char extension_file[4];	
	size_t total_sub_arquivos = atoi(xml_c.total_sub_arquivos);
	msg_log = NULL;
	msg_log = malloc(BUFFER_SIZE);
	pthread_t pth;
	
	strcpy(path_file, "/tmp/xml/XML_");
	strcpy(extension_file, ".xml");	
	strcat(path_file, xml_c.id_login);
	strcat(path_file, "_1");
	strcat(path_file, extension_file);
	
	exists = file_exists(path_file);
	if(exists == 1)
	{
		printf("SUB_ARQUIVO '%s' JA EXISTE\n", path_file);		
		strcpy(msg_log, "SUB_ARQUIVO_EXISTE: ");
		strcat(msg_log, path_file);
		strcat(msg_log, "\n");
		write_log("LOG_ERR", msg_log);
		free(msg_log);
	}
	else
	{	
		char *data = open_url(xml_c.url);		
		size_t inicio = 0;
		size_t division_string = strlen(data) / total_sub_arquivos;//Qtd de caracteres de cada subarquivo xml.
		size_t fim = division_string;
		char* content = (char *) malloc(division_string*sizeof(char));
		thread_parm_t *param=NULL;
		param = malloc(BUFFER_SIZE*sizeof(char));
		
		printf("%s\n", path_file);
			
		/*content = get_block_struct(data, inicio, division_string, 0);
		write_file((const char *)path_file, content);*/		
		param->xml_pointer = data;
		param->id_login = xml_c.id_login;
		param->url = xml_c.url;
		param->inicio = inicio;
		param->fim = division_string;
		param->iterat = 0;
		pthread_create(&pth, NULL, get_block_struct, (void *)param);
		pthread_join(pth,NULL);
		if(b_content.content != NULL)		
			write_file((const char *)path_file, b_content.content);	
		b_content.content = NULL;
		
		strcpy(path_file, "");	
		strcpy(path_file, "/tmp/xml/XML_");
		strcpy(extension_file, ".xml");
			
		for( i = 2; i <= total_sub_arquivos; i++ )
		{
			snprintf(char_i, 10,"%d",i);		
			strcat(path_file, xml_c.id_login);
			strcat(path_file, underline);
			strcat(path_file, extension_file);
			
			printf("%s\n", path_file);
			
			inicio = inicio + division_string;
			/*content = get_block_struct(data, inicio, fim, 0);		
			write_file((const char *)path_file, content);*/					
			param->inicio = inicio;
			param->fim = fim;
			param->iterat = 0;
			
			pthread_create(&pth, NULL, get_block_struct, (void *)param);
			pthread_join(pth,NULL);
			if(b_content.content != NULL)		
				write_file((const char *)path_file, b_content.content);
			b_content.content = NULL;
				
			strcpy(path_file, "/tmp/xml/XML_");
		}
		free(data);
		free(content);
	}
}

/**
 * Funcao para tratar os erros do socket.
 * Ainda nao esta implementada.
 **/
void error_socket(const char *msg)
{
    perror(msg);
}
/**
 * Le entradas via socket.
 * Funcao que viabiliza a conversa entre essa rotina e a feita em python.
 **/ 
int read_socket(int argc, char **argv)
{	
	int sockfd, newsockfd, portno;
	socklen_t clilen;
	char buffer[256];
	struct sockaddr_in serv_addr, cli_addr;
	int n;
	if (argc < 3) 
	{
		fprintf(stderr,"usage %s port_in port_out\n", argv[0]);
		exit(1);
	}
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0)
	{
		error_socket("ERROR opening socket");
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	portno = atoi(argv[1]);
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(portno);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
	{
		error_socket("ERROR on binding");
	}
	listen(sockfd,5);
	clilen = sizeof(cli_addr);
	newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
	if (newsockfd < 0)
	{
		error_socket("ERROR on accept");
	}
	bzero(buffer,256);
	n = read(newsockfd,buffer,255);
	if (n < 0)
	{
		error_socket("ERROR reading from socket");
	}
	
	printf("Here is the message: %s\n",buffer);
	if(strcmp(buffer,"xml_liberado") == 0)
	{
		n = write(newsockfd,"I got your message",18);
		if (n < 0)
		{
			error_socket("ERROR writing to socket");
		}
		close(newsockfd);
		close(sockfd);
		return 1;
	}
	else
	{
		error_socket("ERROR buffer != xml_liberado");
	}
	return 0;
}
/**
 * Escreve saidas via socket.
 * Funcao que viabiliza a conversa entre essa rotina e a feita em python.
 **/ 
void write_in_socket(int argc, char **argv)
{
	int sockfd, portno, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;
	const char* hostname = "localhost";
    char buffer[256];
    
    if (argc < 3) 
    {
       fprintf(stderr,"usage %s port_in port_out\n", argv[0]);
       exit(0);
    }
    portno = atoi(argv[2]);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        error_socket("ERROR opening socket");
	}	
    server = gethostbyname(hostname);
    if (server == NULL)
	{
        fprintf(stderr,"ERROR, no such host\n");
        exit(0);
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0)
    {
        error_socket("ERROR connecting");
	}
    printf("Please enter the message: ");
    bzero(buffer,256);
    
    //fgets(buffer,255,stdin);
    strcpy(buffer, "sub_arquivos_liberados");
    n = write(sockfd,buffer,strlen(buffer));
    
    if (n < 0)
    {
         error_socket("ERROR writing to socket");
	}
    bzero(buffer,256);
    n = read(sockfd,buffer,255);
    if (n < 0)
    {
         error_socket("ERROR reading from socket");
	}
    printf("%s\n",buffer);
    close(sockfd);
}

int main(int argc, char **argv) 
{
	//parse_xml_conf();
	/*
	int libera_parse;
	libera_parse = read_socket(argc, argv);
	if(libera_parse == 1)
	{
		parse_xml_conf();
		write_in_socket(argc, argv);
	}
	* */
	if(argc > 2)
	{
		int libera_parse;
		daemonize();
		while (1) {			
			libera_parse = read_socket(argc, argv);
			if(libera_parse == 1)
			{
				parse_xml_conf();
				write_in_socket(argc, argv);
			}
			sleep(300);
		}
	}
	else
	{
		printf("Usage: %s [port]\n",argv[0]);
		exit(0);
	}
	return 0;
}

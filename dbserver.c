#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <dirent.h>
#include "string.h"
char files[60][30];

int listFiles() {
    struct dirent *dp;
    DIR *dir = opendir("\\tmp\\final_project\\");

    if (!dir)
        return;

    int i = 0;
    while ((dp = readdir(dir)) != NULL) {
        strcpy(files[i], dp->d_name);
        i++;
    }
    closedir(dir);

    return i;
}

void do_exit(PGconn *conn, PGresult *res) {

    fprintf(stderr, "%s\n", PQerrorMessage(conn));

    PQclear(res);
    PQfinish(conn);

    exit(1);
}


int main() {
    int i,j,k;
    int n = listFiles();

    PGconn *conn = PQsetdbLogin("localhost",
                                "5432", "", "",
                                "fpdb", "postgres", "");

    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
                PQerrorMessage(conn));
    }

    for (i = 2; i < n; ++i) {
        char s[70] = "\\tmp\\final_project\\";
        char param[8][20];
        FILE *ftp = fopen(strcat(s,files[i]),"r");
        while (!feof(ftp)){
            char buff[200];
            fgets(buff, 200, ftp);
            char * token = strtok(buff, ",");
            int i = 0;
            while( token != NULL ) {
                strcpy(param[i], token);
                i++;
                token = strtok(NULL, ",");
            }
            for (int l = 0; l < i; ++l) {

            }
            res = PQexec(conn, "INSERT INTO fp_stores_data VALUES(,'Audi',52642)");

            if (PQresultStatus(res) != PGRES_COMMAND_OK)
                do_exit(conn, res);

            PQclear(res);
        }
        fclose(ftp);
    }

    PQfinish(conn);

    return 0;
}



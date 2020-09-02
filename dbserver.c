#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include "libpq-fe.h"
#include "string.h"

char files[60][30];

int listFiles() {
    struct dirent *dp;
    DIR *dir = opendir("C:\\Users\\MHJ\\Desktop\\new");
    if (!dir)
        return 0;

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
    int i, j, k;
    int n = listFiles();
    PGconn *conn = PQsetdbLogin("localhost",
                                "5432", "", "",
                                "fpdb", "postgres", "");
    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
                PQerrorMessage(conn));
    }
    for (i = 2; i < n; ++i) {
        char s[70] = "C:\\Users\\MHJ\\Desktop\\new\\";
        char param[8][40];
        FILE *ftp = fopen(strcat(s, files[i]), "r");
        while (!feof(ftp)) {
            char buff[200];
            fgets(buff, 200, ftp);
            char *token = strtok(buff, ",");
            int i = 0;
            while (token != NULL) {
                strcpy(param[i], token);
                i++;
                token = strtok(NULL, ",");
            }
            i--;
            int l;
            for (l = 0; l < i+1; ++l) {
                if (l == i ) {
                    strcat(param[l], "');");
                } else {
                    strcat(param[l], "','");
                }
            }

            char order[300] = "INSERT INTO fp_stores_data (time, province, city, market_id, product_id, price, quantity, has_sold) VALUES('";

            for (l = 0; l < i+1; ++l) {
                strcat(order, param[l]);
            }

            PGresult *res = PQexec(conn, order);

            if (PQresultStatus(res) != PGRES_COMMAND_OK)
                do_exit(conn, res);

            PQclear(res);

        }
        fclose(ftp);
    }

    PQfinish(conn);

    return 0;
}



#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include "libpq-fe.h"
#include "string.h"

char files[60][30];
int time, quantity, has_sold, totalprice;
char city[40] = "", market_id[20] = "";

void insert_stores_data(char param[8][40], PGconn *conn); //function for inserting data into fp_stores_data

void insert_city(char param[8][40], PGconn *conn); //function for inserting data into fp_city_aggregation

void insert_market(char param[8][40], PGconn *conn); //function for inserting data into fp_store_aggergation

int listFiles() {
    //function for collecting names of the files
    struct dirent *dp;
    DIR *dir = opendir("C:\\Users\\MHJ\\Desktop\\new");
    if (!dir)
        return 0;

    int i = 0;
    while ((dp = readdir(dir)) != NULL) {
        strcpy(files[i], dp->d_name); //files are the names of the reports files in the folder
        i++;
    }
    closedir(dir);

    return i;
}

void do_exit(PGconn *conn, PGresult *res) {
    //exit function when sth goes wrong while inserting data
    fprintf(stderr, "%s\n", PQerrorMessage(conn));

    PQclear(res);
    PQfinish(conn);

    exit(1);
}

int main() {
    int i, j, k;
    int n = listFiles(); //the output of listfiles is the number of the files in the folder
    //connenting to the database
    PGconn *conn = PQsetdbLogin("localhost",
                                "5432", "", "",
                                "fpdb", "postgres", "");
    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
                PQerrorMessage(conn));
    }
    for (i = 2; i < n; ++i) { //for every file in the folder
        char s[70] = "C:\\Users\\MHJ\\Desktop\\new\\";
        char param[8][40];
        FILE *ftp = fopen(strcat(s, files[i]), "r");
        while (!feof(ftp)) { //while for reading all lines
            char buff[200];
            fgets(buff, 200, ftp);
            char *token = strtok(buff, ","); //dividing the lines
            int i = 0;
            while (token != NULL) {
                strcpy(param[i], token);
                i++;
                token = strtok(NULL, ",");
            }
            //parameters are diifferent parts of the line
            insert_stores_data(param, conn);
            insert_city(param, conn);
            insert_market(param, conn);
        }
        fclose(ftp);
    }

    PQfinish(conn);

    return 0;
}

void insert_stores_data(char param[8][40], PGconn *conn) {
    int l;
    char hold1[40], hold2[40], hold3[40];
    strcpy(hold1, param[0]);
    strcpy(hold2, param[2]);
    strcpy(hold3, param[3]);
    for (l = 0; l < 8; ++l) {
        if (l == 7) {
            strcat(param[l], "');");
        } else {
            strcat(param[l], "','");
        }
    }

    char order[300] = "INSERT INTO fp_stores_data (time, province, city, market_id, product_id, price, quantity, has_sold) VALUES('";

    for (l = 0; l < 8; ++l) {
        strcat(order, param[l]);
    }

    PGresult *res = PQexec(conn, order); //inserting into table

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        do_exit(conn, res);

    PQclear(res);

    strcpy(param[0], hold1);
    strcpy(param[2], hold2);
    strcpy(param[3], hold3);
}

void insert_city(char param[8][40], PGconn *conn) {
    char hold1[40];
    strcpy(hold1, param[0]);
    if (strcmp(city, param[2]) == 0) {
        quantity += atoi(param[6]);
        has_sold += atoi(param[7]);
        totalprice += atoi(param[6]) * atoi(param[5]);
    } else {
        if (strcmp(city, "") != 0) {
            char q[20], h[20], p[20];
            itoa(quantity, q, 10);
            itoa(has_sold, h, 10);
            itoa(totalprice / quantity, p, 10);

            char order[300] = "INSERT INTO fp_city_aggregation (time, city, price, quantity, has_sold) VALUES('";

            strcat(param[0], "','");
            strcat(order, param[0]);
            strcat(city, "','");
            strcat(order, city);
            strcat(p, "','");
            strcat(order, p);
            strcat(q, "','");
            strcat(order, q);
            strcat(h, "');");
            strcat(order, h);

            PGresult *res = PQexec(conn, order);//inserting into table

            if (PQresultStatus(res) != PGRES_COMMAND_OK)
                do_exit(conn, res);

            PQclear(res);
        }
        quantity = atoi(param[6]);
        has_sold = atoi(param[7]);
        totalprice = atoi(param[6]) * atoi(param[5]);
        strcpy(city, param[2]);
        strcpy(param[0], hold1);
    }
}

void insert_market(char param[8][40], PGconn *conn) {
    if (strcmp(market_id, param[3]) == 0) {
        quantity += atoi(param[6]);
        has_sold += atoi(param[7]);
        totalprice += atoi(param[6]) * atoi(param[5]);
    } else {
        if (strcmp(market_id, "") != 0) {
            char q[20], h[20], p[20];
            itoa(quantity, q, 10);
            itoa(has_sold, h, 10);
            itoa(totalprice / quantity, p, 10);

            char order[300] = "INSERT INTO fp_store_aggregation (time, market_id, price, quantity, has_sold) VALUES('";
            strcat(param[0], "','");
            strcat(order, param[0]);
            strcat(market_id, "','");
            strcat(order, market_id);
            strcat(p, "','");
            strcat(order, p);
            strcat(q, "','");
            strcat(order, q);
            strcat(h, "');");
            strcat(order, h);
            puts(order);
            PGresult *res = PQexec(conn, order);//inserting into table

            if (PQresultStatus(res) != PGRES_COMMAND_OK)
                do_exit(conn, res);

            PQclear(res);
        }
        quantity = atoi(param[6]);
        has_sold = atoi(param[7]);
        totalprice = atoi(param[6]) * atoi(param[5]);
        strcpy(market_id, param[3]);
    }
}




















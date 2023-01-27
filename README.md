# dadoslegiveis-backend

## About The Project

The goal of this project is to provide an interactive way for the public to understand the expenses of Brazilian deputies by presenting the data in a visual format.

## Built With

This backend project is built using FastAPI, PostgreSQL and Redis. 
The API endpoints are designed to handle requests from the frontend and communicate with the database to retrieve and update the expenses data. 
PostgreSQL is used as the primary data store and Redis is used to handle caching and improve the performance of expensive queries. 
The backend also includes a scheduled task that runs daily to update the database with the latest expenses data from the open government website (Dados Abertos da Câmara dos Deputados). 
The project uses the psycopg2 library to interact with the PostgreSQL database and it runs in two separated containers in AWS ECS, 
one is running the redis server and the other the fastapi server.

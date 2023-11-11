# Delta-PubSub-Jupyter-Practice
**This repo creates a delta table in a Jupyter notebook, publishes the table to pub/sub, and reads/acknowledges the messages back from pub/sub.**

### Prerequisites
1.  **WSL and Docker**
Windows: [Install WSL](https://learn.microsoft.com/en-us/windows/wsl/install) (Windows Subsystem for Linux) to run docker. [Download docker desktop](https://www.docker.com/products/docker-desktop/). Follow the installation wizard and open the app.
Mac: [Install instructions](https://docs.docker.com/desktop/install/mac-install/)

2. **GCP (Google Cloud Platform)**
A GCP project and service account with a pub/sub topic is required for this notebook. The `.json` file associated with the service account is used to publish messages to pub/sub. 

### Running the container:

1. Open a terminal (Powershell or equivalent) and clone the repo to a location of your choice.

2. `cd` into the `delta-pubsub-jupyter-practice` file. 

3. Open the project in VS Code using  `code .` or another editor of your choice.

4. ***Optional:*** edit the `ACCESS_TOKEN` in the `.env` file `delta-pubsub-jupyter-practice/.env`

5. In the terminal, use `docker compose -f jupyter-local-spark-compose.yml up` to start your container.  Use the `-d` flag to start your container in detached mode.

6. The container should now be running and viewable in your docker desktop app.

7. Open [localhost:8889](http://localhost:8889/) in your browser and enter the `ACCESS_TOKEN` stored in the `.env` file to log in. 

8. Open the `PracticeWriteRead.ipynb` file to run/edit the Jupyter notebook. The notebook needs to be run twice in to show that the messages have been recieved back from pub/sub.

9. Use `docker compose -f jupyter-local-spark-compose.yml down` to tear down your deployment when you are finished using it. 



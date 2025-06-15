FROM ubuntu:latest

# Update package list Before you install any app, you want to check the App Store to see which apps are available and their latest versions.
RUN apt-get update

# Install curl and software-properties-common  installing curl in the app store updated version
RUN apt-get install -y curl

# Install Node.js (version 20.x)
# this install the adds Node.js 20.x's repository to your system’s package sources.
RUN curl -sL https://deb.nodesource.com/setup_20.x | bash - 
#  and this install 
RUN apt-get install -y nodejs

# Install Git
RUN apt-get install -y git

# Set the working directory
WORKDIR /home/app

# Copy your script into the container This copies main.sh from your local machine (host) into /home/app/main.sh
COPY main.sh main.sh
COPY script.js script.js
COPY package*.json .

RUN npm install
# Make the script executable (optional but recommended)
RUN chmod +x main.sh
RUN chmod +x script.js


# Define the entry point
ENTRYPOINT [ "/home/app/main.sh" ]

mvn package
scp -i ~/.ssh/awsrob_sshkey.pem target/BioInfPrj-0.0.1-SNAPSHOT.jar ubuntu@18.202.241.92:~/ 
 
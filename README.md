# HadoopFinalAssignmentHealth2
Final Project for Dezyre class -- contains Hive, Pig, and MapReduce scripts processing data from 2011 Medicare database.

List of files:

   inpatient_small.csv                    Sample of dataset used

   Robert Chen Final Explanation.pdf      More detailed explanation of project

   mapreduce/

      mapreduce_notes.txt                 Explanation/notes on the mapreduce portion
 
      InPatient.java                      Code for finding average cost per procedure

      InPatientState.java                 Code for finding average cost per state

      output_by_procedure.txt             Output when running InPatient.java

      output_by_state.txt                 Output when running InPatientState.java

      InPatient.tar                       Full Eclipse workspace to run InPatient

      InPatientState.tar                  Full Eclipse workspace to run InPatientState

      opencsv-2.2.jar                     Custom jar file for processing CSV (see mapreduce_notes.txt)

   pig/

      pig_final_assignment.pig            Commands in pig used to run queries and the results

   hive/

      hive_final_assignment.q             Commands in hive used to run queries and the results

      csv_serde-0.9.1.jar                 Custom serde used for processing CSV (see .q file for detals on use)


   

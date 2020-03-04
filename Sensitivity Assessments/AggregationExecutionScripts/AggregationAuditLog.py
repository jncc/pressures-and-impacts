########################################################################################################################

# Title: Aggregation Audit Log

# Authors: Matear, L.(2020)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    Create a running log and audit trail of the versions of data stored within each iteration of
#                        Aggregation Execution Script processes and any data used to create these outputs.
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                      A. MarESA Preparation: Unknowns Automation                                      #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import os
import time
import numpy as np
import pandas as pd

#############################################################


# Define the code as a function to be executed as necessary
def main():

    #############################################################

    # Start the timer of the function
    start = time.process_time()
    # Create empty list to store the file names for the most recently created MarESA output files
    files_list = []

    #############################################################

    # Load the most recent aggregation output as a string of the file name for Sensitivity
    # Define a directory to be searched
    search_dir_sens = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Sensitivity"
    # Set this as the working directory
    os.chdir(search_dir_sens)
    # Read in all files within this target directory
    files_sens = filter(os.path.isfile, os.listdir(search_dir_sens))
    # Add full filepaths to the identified files within said directory
    files_sens = [os.path.join(search_dir_sens, f) for f in files_sens]
    # Sort all files read in by the most recently edited first
    files_sens.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_sens = files_sens[0]
    # Refine the recent_sens string to only retain the file name
    files_list.append(recent_sens.split('\\')[-1])

    #############################################################

    # Load the most recent aggregation output as a string of the file name for Resistance
    # Define a directory to be searched
    search_dir_res = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resistance"
    # Set this as the working directory
    os.chdir(search_dir_res)
    # Read in all files within this target directory
    files_res = filter(os.path.isfile, os.listdir(search_dir_res))
    # Add full filepaths to the identified files within said directory
    files_res = [os.path.join(search_dir_res, f) for f in files_res]
    # Sort all files read in by the most recently edited first
    files_res.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_res = files_res[0]
    files_list.append(recent_res.split('\\')[-1])

    #############################################################

    # Load the most recent aggregation output as a string of the file name for Resilience
    # Define a directory to be searched
    search_dir_resil = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resilience"
    # Set this as the working directory
    os.chdir(search_dir_resil)
    # Read in all files within this target directory
    files_resil = filter(os.path.isfile, os.listdir(search_dir_resil))
    # Add full filepaths to the identified files within said directory
    files_resil = [os.path.join(search_dir_resil, f) for f in files_resil]
    # Sort all files read in by the most recently edited first
    files_resil.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_resil = files_resil[0]
    files_list.append(recent_resil.split('\\')[-1])

    #############################################################

    # Load the most recent aggregation output as a string of the file name for the BH3 Calculation
    # Define a directory to be searched
    search_dir_BH3 = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\BH3Calculations"
    # Set this as the working directory
    os.chdir(search_dir_BH3)
    # Read in all files within this target directory
    files_BH3 = filter(os.path.isfile, os.listdir(search_dir_BH3))
    # Add full filepaths to the identified files within said directory
    files_BH3 = [os.path.join(search_dir_BH3, f) for f in files_BH3]
    # Sort all files read in by the most recently edited first
    files_BH3.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_BH3 = files_BH3[0]
    files_list.append(recent_BH3.split('\\')[-1])

    #############################################################

    # Load the most recent Annex 1 Sub-type aggregation output as a string of the file name
    # Define a directory to be searched
    search_dir_AnnexI = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs"
    # Set this as the working directory
    os.chdir(search_dir_AnnexI)
    # Read in all files within this target directory
    files_AnnexI = filter(os.path.isfile, os.listdir(search_dir_AnnexI))
    # Add full filepaths to the identified files within said directory
    files_AnnexI = [os.path.join(search_dir_AnnexI, f) for f in files_AnnexI]
    # Sort all files read in by the most recently edited first
    files_AnnexI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_AnnexI = files_AnnexI[0]
    files_list.append(recent_AnnexI.split('\\')[-1])

    #############################################################

    # Load the most recent Annex 1 Sub-type aggregation output as a string of the file name
    # Define a directory to be searched
    search_dir_AnnexIDepth = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation"
    # Set this as the working directory
    os.chdir(search_dir_AnnexIDepth)
    # Read in all files within this target directory
    files_AnnexIDepth = filter(os.path.isfile, os.listdir(search_dir_AnnexIDepth))
    # Add full filepaths to the identified files within said directory
    files_AnnexIDepth = [os.path.join(search_dir_AnnexIDepth, f) for f in files_AnnexIDepth]
    # Sort all files read in by the most recently edited first
    files_AnnexIDepth.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    # Define a string variable of the most recent sensitivity output file
    recent_AnnexIDepth = files_AnnexIDepth[0]
    files_list.append(recent_AnnexIDepth.split('\\')[-1])

    #############################################################

    #  Add files into the File column of the audit DF to be recorded
    # Load in the existing log file to be updated with the outputs to store metadata
    audit = pd.read_csv(
        r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregation_OutputLog_AuditTrailOnly.csv")

    # Load the newest MarESA Aggregation output files as a Pandas Dataframe with the column set to 'File Name'
    new_files = pd.DataFrame(files_list, columns=['File Name'])

    #############################################################

    # Define function to iterate through the rows of the new_files DF and return the creation date
    def date_split(row):
        # Pull out filepath from the target DF
        filepath = row['File Name']
        # Run date slicing specifically on the MarESA data
        if 'Bioreg' in str(filepath):
            # Slice the target filepath by the underscore to return the date the file was created
            str_slice = str(filepath).split('_')[1]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the BH3 data
        elif 'BH3' in str(filepath):
            # Slice the target filepath by the underscore to return the date the file was created
            str_slice = str(filepath).split('_')[2]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the Annex I data
        elif 'FeatureLevel' in str(filepath):
            # Slice the target filepath by the underscore to return the date the file was created
            str_slice = str(filepath).split('_')[1]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice

    # Run the string_split() function on the new_files DF to acquire the date of interest
    new_files['Date Created'] = new_files.apply(lambda row: date_split(row), axis=1)

    #############################################################

    # Define function to iterate through the rows of the new_files DF and return the Bioregions Extract version used
    def bioreg_split(row):
        # Pull out filepath from the target DF
        filepath = row['File Name']
        # Run date slicing specifically on the MarESA data
        if 'Bioreg' in str(filepath):
            # Slice the target filepath by the underscore to return the date of the Bioregions Extract used
            str_slice = str(filepath).split('_')[2]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the BH3 data
        elif 'BH3' in str(filepath):
            return 'Not Applicable'
        # Run date slicing specifically on the Annex I data
        elif 'FeatureLevel' in str(filepath):
            return 'Not Applicable'

    # Run the string_split() function on the new_files DF to acquire the date of interest
    new_files['Bioregions Extract Used'] = new_files.apply(lambda row: bioreg_split(row), axis=1)

    #############################################################

    # Define function to iterate through the rows of the new_files DF and return the MarESA Extract version used
    def maresa_split(row):
        # Pull out filepath from the target DF
        filepath = row['File Name']
        # Run date slicing specifically on the MarESA data
        if 'Bioreg' in str(filepath):
            # Slice the target filepath by the underscore to return the date of the Bioregions Extract used
            str_slice = str(filepath).split('_')[3]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the BH3 data
        elif 'BH3' in str(filepath):
            return 'Not Applicable'
        # Run date slicing specifically on the Annex I data
        elif 'FeatureLevel' in str(filepath):
            return 'Not Applicable'

    # Run the string_split() function on the new_files DF to acquire the date of interest
    new_files['MarESA Extract Used'] = new_files.apply(lambda row: maresa_split(row), axis=1)

    #############################################################

    # Define function to iterate through the rows of the new_files DF and return the Resistance Aggregation used
    def res_split(row):
        # Pull out filepath from the target DF
        filepath = row['File Name']
        # Run date slicing specifically on the MarESA data
        if 'Bioreg' in str(filepath):
            return 'Not Applicable'
        # Run date slicing specifically on the BH3 data
        elif 'BH3' in str(filepath):
            # Slice the target filepath by the underscore to return the date the file was created
            str_slice = str(filepath).split('_')[3]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the Annex I data
        elif 'FeatureLevel' in str(filepath):
            return 'Not Applicable'

    # Run the string_split() function on the new_files DF to acquire the date of interest
    new_files['Resistance Aggregation Used'] = new_files.apply(lambda row: res_split(row), axis=1)

    #############################################################

    # Define function to iterate through the rows of the new_files DF and return the Resistance Aggregation used
    def resil_split(row):
        # Pull out filepath from the target DF
        filepath = row['File Name']
        # Run date slicing specifically on the MarESA data
        if 'Bioreg' in str(filepath):
            return 'Not Applicable'
        # Run date slicing specifically on the BH3 data
        elif 'BH3' in str(filepath):
            # Slice the target filepath by the underscore to return the date the file was created
            str_slice = str(filepath).split('_')[4]
            # Slice the object again to remove the .csv from the date
            str_slice = str(str_slice).split('.')[0]
            # Return the sliced str_slice data object
            return str_slice
        # Run date slicing specifically on the Annex I data
        elif 'FeatureLevel' in str(filepath):
            return 'Not Applicable'

    # Run the string_split() function on the new_files DF to acquire the date of interest
    new_files['Resilience Aggregation Used'] = new_files.apply(lambda row: resil_split(row), axis=1)

    #############################################################

    # Append the newly created files back into the existing audit DF
    updated_audit = audit.append(new_files)

    updated_audit = updated_audit[[
        'File Name', 'Date Created', 'Bioregions Extract Used', 'MarESA Extract Used', 'Resistance Aggregation Used',
        'Resilience Aggregation Used'
    ]]

    # Export the audit trail document back to the original filepath once updated
    updated_audit.to_csv(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregation_OutputLog_AuditTrailOnly.csv")

main()
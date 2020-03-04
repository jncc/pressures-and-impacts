########################################################################################################################

# Title: Aggregation Execution

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                               Aggregation Preparation:                                               #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required or data manipulation
import os
import time
import numpy as np
import pandas as pd

#############################################################


# Define the code as a function to be executed as necessary
def main():
    # Test the run time of the function
    start = time.process_time()

    # Load all Annex 1 sub-type data into Pandas DF from MS Office .xlsx document
    annex1 = pd.read_excel(r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\AnnexI_sub_types_all_v3LM.xlsx", 'Sheet1', dtype=str)

    # Load all MarESA sensitivity assessment data
    # Define a directory to be searched
    maresa_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Aggregation_InputData\MarESAExtract"
    # Set this as the working directory
    os.chdir(maresa_dir)
    # Read in all files within this target directory
    maresa_files = filter(os.path.isfile, os.listdir(maresa_dir))
    # Add full filepaths to the identified files within said directory
    maresa_files = [os.path.join(maresa_dir, f) for f in maresa_files]
    # Sort all files read in by the most recently edited first
    maresa_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the MarESA object with unknowns - read the last
    # edited file
    maresa = pd.read_csv(maresa_files[0], dtype={'EUNIS_Code': str})

    # Load JNCC Correlations Table data
    CorrelationTable = pd.read_excel(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Unknowns_InputData\CorrelationTable_C22032019.xlsx", 'Correlations', dtype=str)

    # Strip all trailing whitespace from both the annex1 DF and the maresa DF prior to merging
    annex1['EUNIS code'] = annex1['EUNIS code'].str.strip()
    maresa['EUNIS_Code'] = maresa['EUNIS_Code'].str.strip()

    # Merge MarESA sensitivity assessments with all Habitats Directive listed Annex 1 habitats and sub-types of
    # relevancew
    maresa_annex_merge = pd.merge(annex1, maresa, left_on='EUNIS code', right_on='EUNIS_Code', how='outer', indicator=True)

    # Create a subset of the maresa_annex_merge which only contains the listed Annex 1 data
    annex1_only = maresa_annex_merge.loc[maresa_annex_merge['_merge'].isin(['left_only'])]

    # Drop the merge column from the DF as this is no longer needed
    maresa_annex_merge = maresa_annex_merge.drop(['_merge'], axis=1, inplace=False)

    # Reformat contents of assessment columns within DF
    maresa_annex_merge['Sensitivity'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Sensitivity'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Sensitivity'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)

    maresa_annex_merge['Resistance'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Resistance'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Resistance'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)

    maresa_annex_merge['Resilience'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Resilience'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Resilience'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    maresa_annex_merge['Sensitivity'].fillna(value='', inplace=True)

    # Subset the maresa_annex_merge DF to only retain the columns of interest
    maresa_annex_merge = maresa_annex_merge[[
        'JNCC_Code', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone', 'Classification level', 'EUNIS code',
        'EUNIS_Code', 'Biotope name', 'JNCC code', 'JNCC name', 'Pressure', 'Resilience', 'Resistance', 'Sensitivity'
    ]]

    # Define function to enable EUNIS levels to be identified from the target DF
    # Function Title: eunis_col
    def eunis_col(row):
        """User defined function to pull out all entries in EUNIS_Code column and create returns based on string
        slices of the EUNIS data. This must be used with df.apply() and a lambda function.

        e.g. fact_tbl[['Level_1', 'Level_2', 'Level_3',
              'Level_4', 'Level_5', 'Level_6']] = fact_tbl.apply(lambda row: pd.Series(eunis_col(row)), axis=1)"""

        # Create object oriented variable to store EUNIS_Code data
        ecode = str(row['EUNIS_Code'])
        # Create if / elif conditions to produce response dependent on the string length of the inputted data.
        if len(ecode) == 1:
            return ecode[0:1], None, None, None, None, None
        elif len(ecode) == 2:
            return ecode[0:1], ecode[0:2], None, None, None, None
        elif len(ecode) == 4:
            return ecode[0:1], ecode[0:2], ecode[0:4], None, None, None
        elif len(ecode) == 5:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], None, None
        elif len(ecode) == 6:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], None
        elif len(ecode) == 7:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], ecode[0:7]

    # Create individual EUNIS level columns in fact_tbl using a lambda function and apply() method on 'EUNIS_Code'
    # column on DF.
    maresa_annex_merge[['Level_1', 'Level_2', 'Level_3', 'Level_4', 'Level_5', 'Level_6']] = \
        maresa_annex_merge.apply(lambda row: pd.Series(eunis_col(row)), axis=1)

    # Define function which identifies the EUNIS level and returns a numerical value representative of the level
    # - e.g. '5

    # Adding a EUNIS level column to the DF based on the 'EUNIS_Code' column
    # Function Title: eunis_lvl
    def eunis_lvl(row):
        """User defined function to pull out all data from the column 'EUNIS_Code' and return an integer dependant on
        the EUNIS level in response"""

        # Create object oriented variable to store EUNIS_Code data
        ecode = str(row['EUNIS_Code'])
        # Create if / elif conditions to produce response dependent on the string length of the inputted data
        if len(ecode) == 1:
            return '1'
        elif len(ecode) == 2:
            return '2'
        elif len(ecode) == 4:
            return '3'
        elif len(ecode) == 5:
            return '4'
        elif len(ecode) == 6:
            return '5'
        elif len(ecode) == 7:
            return '6'

    # Create new 'EUNIS_Level' column which indicates the numerical value of the EUNIS level by passing the fact_tbl to
    # the eunis_lvl() function.
    maresa_annex_merge['EUNIS_Level'] = maresa_annex_merge.apply(lambda row: eunis_lvl(row), axis=1)

    ####################################################################################################################

    # Level 6 to 5 aggregation (sub-setting)

    # Identify all Level 6 only data
    L6 = pd.DataFrame(maresa_annex_merge.loc[maresa_annex_merge['EUNIS_Level'].isin(['6'])])

    # Identify all Level 5  only data
    L5_orig = pd.DataFrame(maresa_annex_merge.loc[maresa_annex_merge['EUNIS_Level'].isin(['5'])]) # PRESENT

    # Remove unknowns from the L5 data used to identify L6's which do not have assessments at L5 (otherwise, they will
    # not be picked up as the unknown value at L5 will make it look like the data exists at L5 when it does not)
    L5_orig_agg = L5_orig[L5_orig.Sensitivity != 'Unknown']

    # Assign data differences to new object oriented variable using outer merge between data frames
    L56_merge = pd.merge(L6, L5_orig_agg, how='outer', on=['Level_5', 'Pressure'], indicator=True)

    # Filter merged data by left only data to subset EUNIS level 6 data which does not have a level 5 assessment
    L56_diff = pd.DataFrame(L56_merge[L56_merge['_merge'] == 'left_only'])

    # Clean outer merge of excess data from right only channel
    L56_diff = L56_diff.drop([
        'JNCC_Code_y', 'Annex I Habitat_y', 'Annex I sub-type_y', 'Depth zone_y', 'Classification level_y',
        'EUNIS code_y', 'EUNIS_Code_y', 'Biotope name_y', 'JNCC code_y', 'JNCC name_y', 'Resilience_y', 'Resistance_y',
        'Sensitivity_y', 'Level_1_y', 'Level_2_y', 'Level_3_y', 'Level_4_y', 'Level_6_y', 'EUNIS_Level_y', '_merge'
    ], axis=1, inplace=False)

    # Rename columns within L56_diff DF to allow for together function() to work
    L56_diff.columns = [
        'JNCC_Code_x', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone_x', 'Classification level_x',
        'EUNIS code_x', 'EUNIS_Code_x', 'Biotope name_x', 'JNCC code_x', 'JNCC name_x', 'Pressure', 'Resilience_x',
        'Resistance_x', 'Sensitivity_x', 'Level_1_x', 'Level_2_x', 'Level_3_x', 'Level_4_x', 'Level_5', 'Level_6_x',
        'EUNIS_Level_x'
    ]

    ####################################################################################################################

    # Level 6 to 5 aggregation (formatting)

    # Create function which takes the Annex I Feature/SubFeature columns, and combines both entries into a single column
    # This enables the data to be grouped and aggregated using the .groupby() function (does not support multiple
    # simultaneous aggregations)
    def together(row):
        # Pull in data from both columns of interest
        ann1 = row['Annex I Habitat']
        sub_f = row['Annex I sub-type']
        # Return a string of both individual targets combined by a ' - ' symbol
        return str(str(ann1) + ' - ' + str(sub_f))

    # Run the together() function to combine Feature/SubFeature data into a single 'FeatureSubFeature' column to be
    # aggregated
    L56_diff['FeatureSubFeature'] = L56_diff.apply(lambda row: together(row), axis=1)

    # The following body of code begins the initial steps of the aggregation process from level 6 to level 5

    # Group data by Level_5, Pressure, SubregionName and apply sensitivity values to list using lambdas function and
    # .apply() method
    L5_agg = L56_diff.groupby(['Level_5', 'Pressure', 'FeatureSubFeature']
                              )['Sensitivity_x'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L5_agg = pd.DataFrame(L5_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L5_agg = L5_agg.reset_index(inplace=False)

    # Reset columns within L5_agg DF
    L5_agg.columns = ['Level_5', 'Pressure', 'FeatureSubFeature', 'Sensitivity']

    # Define function to count the number of assessment values

    # Define all functions which are required within the script to execute aggregation process
    # Function Title: counter
    def counter(value):
        """Count the total no. of occurrences of each sensitivity (high, medium, low, not sensitive, not relevant,
          no evidence, not assessed, unknown
          Return values to be assigned to new columns through lambda function"""
        counthigh = value.count('High')
        countmedium = value.count('Medium')
        countlow = value.count('Low')
        countns = value.count('Not sensitive')
        countnr = value.count('Not relevant')
        countne = value.count('No evidence')
        countna = value.count('Not assessed')
        countuk = value.count('Unknown')
        return counthigh, countmedium, countlow, countns, countnr, countne, countna, countuk

    # Apply the counter() function to the DF to count the occurrence of all assessment values
    L5_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L5_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later in code
    L5_agg['Count_High'] = L5_agg['High']
    L5_agg['Count_Medium'] = L5_agg['Medium']
    L5_agg['Count_Low'] = L5_agg['Low']
    L5_agg['Count_NotSensitive'] = L5_agg['Not sensitive']
    L5_agg['Count_NotRel'] = L5_agg['Not relevant']
    L5_agg['Count_NoEvidence'] = L5_agg['No evidence']
    L5_agg['Count_NotAssessed'] = L5_agg['Not assessed']
    L5_agg['Count_Unknown'] = L5_agg['Unknown']

    # Reassign L5_agg DataFrame to L5_sens for sensitivity aggregation
    L5_sens = L5_agg

    # Create colNames list for use with replacer() function
    colNames = ['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown']

    # Define replacer() function to fill a numerical with the repstring being analysed
    # Function Title: replacer
    def replacer(value, repstring):
        """Perform string replace on each sensitivity count column (one set of duplicates only)"""
        if value == 0:
            return 'NA'
        elif value != 0:
            return repstring

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L5_sens[eachCol] = L5_sens[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 6 to 5 aggregation (aggregation)

    # Aggregate data using the functions defined in section 2b

    # Define function to create a sensitivity value and return a string instance
    # Function Title: create_sensitivity
    def create_sensitivity(df):
        """Series of conditional statements which return a string value of all assessment values
        contained within individual columns"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
        nsens = df['Not sensitive']
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function
        value = []
        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled
        if 'High' in high:
            h = 'High'
            value.append(h)
        if 'Medium' in med:
            m = 'Medium'
            value.append(m)
        if 'Low' in low:
            lo = 'Low'
            value.append(lo)
        if 'Not sensitive' in nsens:
            ns = 'Not sensitive'
            value.append(ns)
        if 'Not relevant' in nrel:
            nr = 'Not relevant'
            value.append(nr)
        if 'No evidence' in nev:
            ne = 'No evidence'
            value.append(ne)
        if 'Not assessed' in n_ass:
            nass = 'Not assessed'
            value.append(nass)
        if 'Unknown' in un:
            unk = 'Unknown'
            value.append(unk)
        s = ', '.join(value)
        return str(s)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    L5_sens['L5_Sensitivity'] = L5_sens.apply(lambda df: create_sensitivity(df), axis=1)

    # Function Title: final_sensitivity
    def final_sensitivity(df):
        """Create a return of a string value which gives final sensitivity score dependent on conditional statements"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
        nsens = df['Not sensitive']
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function
        value = []
        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled
        if 'High' in high:
            h = 'High'
            value.append(h)
        if 'Medium' in med:
            m = 'Medium'
            value.append(m)
        if 'Low' in low:
            lo = 'Low'
            value.append(lo)
        if 'Not sensitive' in nsens:
            ns = 'Not sensitive'
            value.append(ns)
        if 'High' not in high and 'Medium' not in med and 'Low' not in low and 'Not sensitive' not in nsens:
            if 'Not relevant' in nrel:
                nr = 'Not relevant'
                value.append(nr)
            if 'No evidence' in nev:
                ne = 'No evidence'
                value.append(ne)
            if 'Not assessed' in n_ass:
                nass = 'Not assessed'
                value.append(nass)
        if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel and 'NA' in nev and \
                'NA' in n_ass:
            un = 'Unknown'
            value.append(un)

        s = ', '.join(value)
        return str(s)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L5_sens['L5_FinalSensitivity'] = L5_sens.apply(lambda df: final_sensitivity(df), axis=1)

    # Define function to calculate the total count of all assessed values
    # Function Title: combine_assessedcounts
    def combine_assessedcounts(df):
        """Conditional statements which combine assessed count data and return as string value"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
        nsens = df['Not sensitive']
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function
        value = []
        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled
        if 'High' in high:
            h = 'H(' + str(df['Count_High']) + ')'
            value.append(h)
        if 'Medium' in med:
            m = 'M(' + str((df['Count_Medium'])) + ')'
            value.append(m)
        if 'Low' in low:
            lo = 'L(' + str(df['Count_Low']) + ')'
            value.append(lo)
        if 'Not sensitive' in nsens:
            ns = 'NS(' + str(df['Count_NotSensitive']) + ')'
            value.append(ns)
        if 'Not relevant' in nrel:
            nr = 'NR(' + str(df['Count_NotRel']) + ')'
            value.append(nr)
        if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel:
            # if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens:
            # if 'Not relevant' in nrel:
            #     nr = 'Not Applicable'
            #     value.append(nr)
            if 'No evidence' in nev:
                ne = 'Not Applicable'
                value.append(ne)
            if 'Not assessed' in n_ass:
                nass = 'Not Applicable'
                value.append(nass)
            if 'Unknown' in un:
                unk = 'Not Applicable'
                value.append(unk)
        s = ', '.join(value)
        return str(s)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L5_sens['L5_AssessedCount'] = L5_sens.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Define function to calculate the total count of all unassessed values
    # Function Title: combine_unassessedcounts
    def combine_unassessedcounts(df):
        """Conditional statements which combine unassessed count data and return as string value"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function

        values = []

        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled

        # if 'Not relevant' in nrel:
        #     nr = 'NR(' + str(df['Count_NotRel']) + ')'
        #     values.append(nr)
        if 'No evidence' in nev:
            ne = 'NE(' + str(df['Count_NoEvidence']) + ')'
            values.append(ne)
        if 'Not assessed' in n_ass:
            na = 'NA(' + str(df['Count_NotAssessed']) + ')'
            values.append(na)
        if 'Unknown' in un:
            unk = 'UN(' + str(df['Count_Unknown']) + ')'
            values.append(unk)
        # if 'NA' in nrel and 'NA' in nev and 'NA' in n_ass and 'NA' in un:
        if 'NA' in nev and 'NA' in n_ass and 'NA' in un:
            napp = 'Not Applicable'
            values.append(napp)
        s = ', '.join(values)
        return str(s)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L5_sens['L5_UnassessedCount'] = L5_sens.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Define function to sample the level 5 EUNIS code and calculate the relevant level 4 code
    # Function Title: column4
    def column4(df):
        """Sample Level_5 column and return string variables sliced within the range [0:5]"""
        value = df['Level_5']
        sample = value[0:5]
        return sample

    # Apply column4() function to L5_sens DataFrame to create new Level_4 column
    L5_sens['Level_4'] = L5_sens.apply(lambda df: pd.Series(column4(df)), axis=1)

    # Define function to create confidence value for the aggregation process
    # Function Title: create_confidence
    def create_confidence(df):
        """Divide the total assessed counts by the total count of all data and return as numerical value"""
        # Pull in assessed values counts
        count_high = df['Count_High']
        count_med = df['Count_Medium']
        count_low = df['Count_Low']
        count_ns = df['Count_NotSensitive']

        # Pull in unassessed values counts
        count_nr = df['Count_NotRel']
        count_ne = df['Count_NoEvidence']
        count_na = df['Count_NotAssessed']
        count_unk = df['Count_Unknown']

        # Create ratio calculation
        total_ass = count_high + count_med + count_low + count_ns
        total = total_ass + count_ne + count_na + count_unk

        return total_ass / total if total else 0

    # Use lambda function to apply create_confidence() function to the DataFrame
    L5_sens['L5_AggregationConfidenceValue'] = L5_sens.apply(lambda df: create_confidence(df), axis=1)

    ####################################################################################################################

    # Level 6 to 5 aggregation (combining aggregated and existing assessments)

    # Create object oriented variables to be used within section 4c (original L5 data and L5 aggregated data)
    L5_agg_sens = L5_sens
    L5_orig_sub = L5_orig

    # Run the together() function to combine Feature/SubFeature data into a single 'FeatureSubFeature' column to be
    # aggregated on the L5_orig_sub DF
    L5_orig_sub['FeatureSubFeature'] = L5_orig_sub.apply(lambda row: together(row), axis=1)

    # Drop unwanted columns to allow for both DataFrames to have matching indices
    L5_agg_sens = L5_agg_sens.drop([
        'Sensitivity', 'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    # Rename newly created L5_sensitivity column 'Sensitivity' to match both DataFrames
    L5_agg_sens.rename(columns={'L5_Sensitivity': 'Sensitivity'}, inplace=True)

    # Drop unwanted columns from 'L5_orig_sub' DataFrame
    L5_orig_sub = L5_orig_sub.drop([
        'EUNIS_Code', 'Level_1', 'Level_2', 'Level_3', 'Level_4', 'Level_6', 'EUNIS_Level', 'JNCC_Code',
        'Annex I Habitat', 'Annex I sub-type'
    ], axis=1, inplace=False)

    # Apply the counter() function to the L5_orig_sub DataFrame to count the occurrence of all assessment values
    L5_orig_sub[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
                 'Unknown']] = L5_orig_sub.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all L5_orig_sub count values and assign to new columns to be replaced by string values later in code
    L5_orig_sub['Count_High'] = L5_orig_sub['High']
    L5_orig_sub['Count_Medium'] = L5_orig_sub['Medium']
    L5_orig_sub['Count_Low'] = L5_orig_sub['Low']
    L5_orig_sub['Count_NotSensitive'] = L5_orig_sub['Not sensitive']
    L5_orig_sub['Count_NotRel'] = L5_orig_sub['Not relevant']
    L5_orig_sub['Count_NoEvidence'] = L5_orig_sub['No evidence']
    L5_orig_sub['Count_NotAssessed'] = L5_orig_sub['Not assessed']
    L5_orig_sub['Count_Unknown'] = L5_orig_sub['Unknown']

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L5_orig_sub[eachCol] = L5_orig_sub[eachCol].apply(lambda x: replacer(x, eachCol))

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L5_orig_sub['L5_FinalSensitivity'] = L5_orig_sub.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L5_orig_sub['L5_AssessedCount'] = L5_orig_sub.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L5_orig_sub['L5_UnassessedCount'] = L5_orig_sub.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Apply column4() function to L5_orig_sub DataFrame to create new Level_4 column
    L5_orig_sub['Level_4'] = L5_orig_sub.apply(lambda df: pd.Series(column4(df)), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L5_orig_sub['L5_AggregationConfidenceValue'] = L5_orig_sub.apply(lambda df: create_confidence(df), axis=1)

    # Drop all unwanted columns from L5_orig_sub DataFrame
    L5_orig_sub.drop([
        'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence',
        'Not assessed', 'Unknown', 'Count_High', 'Count_Medium', 'Count_Low', 'Count_NotSensitive',
        'Count_NotRel', 'Count_NoEvidence', 'Count_NotAssessed', 'Count_Unknown'
    ], axis=1, inplace=True)

    # Append L5_orig_sub DataFrame with newly developed L5_agg_sens DataFrame
    L5_all = pd.merge(L5_orig_sub, L5_agg_sens, how='outer')

    # Format columns into correct order
    L5_all = L5_all[[
        'Pressure', 'FeatureSubFeature', 'Level_4', 'Level_5', 'Sensitivity', 'L5_FinalSensitivity',
        'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AggregationConfidenceValue'
    ]]

    ####################################################################################################################

    # Aggregation of data by Annex 1 Sub-feature (SETUP)

    # Create DataFrame for master DataFrame at end of script
    annex_aggregation = L5_all

    # Drop unwanted columns from L5_export DataFrame
    annex_aggregation = annex_aggregation.drop(['Sensitivity'], axis=1, inplace=False)

    # Rename and reorder columns within annex_aggregation
    annex_aggregation = annex_aggregation[
        [
            'Pressure', 'FeatureSubFeature', 'Level_5', 'L5_FinalSensitivity', 'L5_AssessedCount',
            'L5_UnassessedCount', 'L5_AggregationConfidenceValue'
        ]
    ]

    # Check for all data which have EUNIS codes, but do not have a correlating Annex 1 Feature/SubFeature
    eunis_only_notSNH = annex_aggregation.loc[annex_aggregation['FeatureSubFeature'].isin(['nan - nan'])]

    # Create list of all unique EUNIS biotope codes which do not have an associated Annex 1 Feature / SubFeature
    eunis_no_SNH_feature = eunis_only_notSNH['Level_5'].unique()

    # Perform cross-check with the JNCC Correlations Table to identify all EUNIS data which are not included within the
    # Annex 1 Feature/SubFeature DF, but do have a correlating SNH Feature SubFeature value

    # Subset the JNCC Correlation Table to only contain EUNIS codes listed within the eunis_no_SNH_feature DF
    correlation_subset = CorrelationTable.loc[CorrelationTable['EUNIS code 2007'].isin(eunis_no_SNH_feature)]

    # Refine the correlation_subset DF to only include data where the'SNH Annex I sub-type' column is not null
    missing_data = correlation_subset.loc[~correlation_subset['SNH Annex I sub-type'].astype(str).isin(['nan'])]

    pd.options.display.max_colwidth = 100
    print("The Feature / SubFeature input data are missing the following entries: " + '\n' +
          'EUNIS Code: ' + str(missing_data['EUNIS code 2007']) + '\n' +
          'SNH Annex I sub-type: ' + str(missing_data['SNH Annex I sub-type']) + '\n' +
          'EUNIS Name: ' + str(missing_data['EUNIS name 2007']))

    ####################################################################################################################

    # Aggregation of data by Annex 1 Sub-feature (AGGREGATION)

    # Group data by Level_5, Pressure, SubregionName and apply sensitivity values to list using lambdas function and
    # .apply() method
    feature_sub_aggregation = annex_aggregation.groupby(
        ['Pressure', 'FeatureSubFeature']
    )['L5_FinalSensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    feature_sub_aggregation = pd.DataFrame(feature_sub_aggregation)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    feature_sub_aggregation = feature_sub_aggregation.reset_index(inplace=False)

    # Reset columns within L5_agg DF
    feature_sub_aggregation.columns = ['Pressure', 'FeatureSubFeature', 'Sensitivity']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    feature_sub_aggregation[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
                             'Unknown']] = \
        feature_sub_aggregation.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    feature_sub_aggregation['Count_High'] = feature_sub_aggregation['High']
    feature_sub_aggregation['Count_Medium'] = feature_sub_aggregation['Medium']
    feature_sub_aggregation['Count_Low'] = feature_sub_aggregation['Low']
    feature_sub_aggregation['Count_NotSensitive'] = feature_sub_aggregation['Not sensitive']
    feature_sub_aggregation['Count_NotRel'] = feature_sub_aggregation['Not relevant']
    feature_sub_aggregation['Count_NoEvidence'] = feature_sub_aggregation['No evidence']
    feature_sub_aggregation['Count_NotAssessed'] = feature_sub_aggregation['Not assessed']
    feature_sub_aggregation['Count_Unknown'] = feature_sub_aggregation['Unknown']

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        feature_sub_aggregation[eachCol] = feature_sub_aggregation[eachCol].apply(lambda x: replacer(x, eachCol))

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    feature_sub_aggregation['Sensitivity'] = feature_sub_aggregation.apply(lambda df: create_sensitivity(df),
                                                                           axis=1)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    feature_sub_aggregation['FinalSensitivity'] = feature_sub_aggregation.apply(lambda df: final_sensitivity(df),
                                                                                axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    feature_sub_aggregation['AssessedCount'] = feature_sub_aggregation.apply(lambda df: combine_assessedcounts(df),
                                                                             axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    feature_sub_aggregation['UnassessedCount'] = feature_sub_aggregation.apply(lambda df: combine_unassessedcounts(df),
                                                                               axis=1)

    # Drop unwanted data from feature_sub_aggregation DataFrame
    feature_sub_aggregation = feature_sub_aggregation.drop([
        'Sensitivity', 'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    ####################################################################################################################

    # Remove all nan - nan values from the FeatureSubFeature column
    feature_sub_aggregation = feature_sub_aggregation.loc[
        ~feature_sub_aggregation['FeatureSubFeature'].isin(eunis_no_SNH_feature)
    ]

    # Split the FeatureSubFeature column back into the individual columns once aggregated by unique combination of
    # Feature and Sub-Feature.

    def str_split(row, str_interval):
        # Import the target column into the local scope of the function
        target_col = row['FeatureSubFeature']
        # Split the target string to get the Feature using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'Feature':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the first of the two list items
            return str(result[0])
        # Split the target string to get the SubFeature using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'SubFeature':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the second of the two list items
            return str(result[1])

    # Run the str_split() function to return the combined Feature data back into two separate columns
    feature_sub_aggregation['Annex I Habitat'] = feature_sub_aggregation.apply(lambda row: str_split(row, 'Feature'),
                                                                               axis=1)

    # Run the str_split() function to return the combined SubFeature data back into two separate columns
    feature_sub_aggregation['Annex I sub-type'] = feature_sub_aggregation.apply(lambda row: str_split(row, 'SubFeature'),
                                                                                axis=1)

    # Rearrange columns correctly into DataFrame schema
    feature_sub_aggregation = feature_sub_aggregation[[
        'Pressure', 'Annex I Habitat', 'Annex I sub-type', 'FinalSensitivity', 'AssessedCount', 'UnassessedCount'
    ]]

    # Remove all data from the DF which is flagged to be NaN within the 'Annex I Habitat' column
    feature_sub_aggregation = feature_sub_aggregation[feature_sub_aggregation['Annex I Habitat'] != 'nan']

    # Export data export
    # Define folder file path to be saved into
    outpath = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs"
    # Define file name to save, categorised by date
    filename = "FeatureLevelAggregation_" + (time.strftime("%Y%m%d") + ".csv")
    # Run the output DF.to_csv method
    feature_sub_aggregation.to_csv(outpath + "\\" + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print("The 'Annex1SubTypeAggregation' script took " + str(round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
          'This has been saved as an output at the following filepath: \\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs')

# Run function / entire script - to be called from AggregationExecution script
main()


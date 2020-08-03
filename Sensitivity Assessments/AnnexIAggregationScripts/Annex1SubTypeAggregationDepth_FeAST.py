########################################################################################################################

# Title: Annex I Sub Type Aggregation Depth FeAST

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
import pandas as pd

#############################################################


# Define the code as a function to be executed as necessary
def main():
    # Test the run time of the function
    start = time.process_time()

    # Load all Annex 1 sub-type data into Pandas DF from MS Office .xlsx document
    annex1 = pd.read_excel(r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\AnnexI_sub_types_all_v6.xlsx", 'L5_BiotopesForAgg', dtype=str)

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
    CorrelationTable = pd.read_excel(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Unknowns_InputData\CorrelationTable_C16042020.xlsx", 'Correlations', dtype=str)

    # Strip all trailing whitespace from both the annex1 DF and the maresa DF prior to merging
    annex1['EUNIS code'] = annex1['EUNIS code'].str.strip()
    maresa['EUNIS_Code'] = maresa['EUNIS_Code'].str.strip()

    # Merge MarESA sensitivity assessments with all Habitats Directive listed Annex 1 habitats and sub-types of
    # relevance
    maresa_annex_merge = pd.merge(annex1, maresa, left_on='EUNIS code', right_on='EUNIS_Code')

    # # TEST
    # test = maresa_annex_merge.loc[maresa_annex_merge['Classification level'].isin(['5'])]
    # test = test.loc[test['Sensitivity'].isin(['Unknown'])]
    # test = test.loc[test['EUNIS code'].isin(['A5.712'])]

    # test['EUNIS code'].unique()
    # Drop the merge column from the DF as this is no longer needed
    # maresa_annex_merge = maresa_annex_merge.drop(['_merge'], axis=1, inplace=False)

    # Reformat contents of assessment columns within DF
    maresa_annex_merge['Sensitivity'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Sensitivity'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Sensitivity'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    maresa_annex_merge['Sensitivity'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    maresa_annex_merge['Resistance'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Resistance'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Resistance'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    maresa_annex_merge['Resistance'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    maresa_annex_merge['Resilience'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    maresa_annex_merge['Resilience'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    maresa_annex_merge['Resilience'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    maresa_annex_merge['Resilience'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    maresa_annex_merge['Sensitivity'].fillna(value='', inplace=True)

    # Subset the maresa_annex_merge DF to only retain the columns of interest
    maresa_annex_merge = maresa_annex_merge[[
        'JNCC_Code', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone', 'Classification level', 'EUNIS code',
        'EUNIS_Code', 'Biotope name', 'JNCC code', 'JNCC name', 'Pressure', 'Resilience', 'Resistance', 'Sensitivity'
    ]]

    # # Define function to enable EUNIS levels to be identified from the target DF
    # # Function Title: eunis_col
    # def eunis_col(row):
    #     """User defined function to pull out all entries in EUNIS_Code column and create returns based on string
    #     slices of the EUNIS data. This must be used with df.apply() and a lambda function.
    #
    #     e.g. fact_tbl[['Level_1', 'Level_2', 'Level_3',
    #           'Level_4', 'Level_5', 'Level_6']] = fact_tbl.apply(lambda row: pd.Series(eunis_col(row)), axis=1)"""
    #
    #     # Create object oriented variable to store EUNIS_Code data
    #     ecode = str(row['EUNIS_Code'])
    #     # Create if / elif conditions to produce response dependent on the string length of the inputted data.
    #     if len(ecode) == 1:
    #         return ecode[0:1], None, None, None, None, None
    #     elif len(ecode) == 2:
    #         return ecode[0:1], ecode[0:2], None, None, None, None
    #     elif len(ecode) == 4:
    #         return ecode[0:1], ecode[0:2], ecode[0:4], None, None, None
    #     elif len(ecode) == 5:
    #         return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], None, None
    #     elif len(ecode) == 6:
    #         return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], None
    #     elif len(ecode) == 7:
    #         return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], ecode[0:7]
    #
    # # Create individual EUNIS level columns in fact_tbl using a lambda function and apply() method on 'EUNIS_Code'
    # # column on DF.
    # maresa_annex_merge[['Level_1', 'Level_2', 'Level_3', 'Level_4', 'Level_5', 'Level_6']] = \
    #     maresa_annex_merge.apply(lambda row: pd.Series(eunis_col(row)), axis=1)
    #
    # # Define function which identifies the EUNIS level and returns a numerical value representative of the level
    # # - e.g. '5
    #
    # # Adding a EUNIS level column to the DF based on the 'EUNIS_Code' column
    # # Function Title: eunis_lvl
    # def eunis_lvl(row):
    #     """User defined function to pull out all data from the column 'EUNIS_Code' and return an integer dependant on
    #     the EUNIS level in response"""
    #
    #     # Create object oriented variable to store EUNIS_Code data
    #     ecode = str(row['EUNIS_Code'])
    #     # Create if / elif conditions to produce response dependent on the string length of the inputted data
    #     if len(ecode) == 1:
    #         return '1'
    #     elif len(ecode) == 2:
    #         return '2'
    #     elif len(ecode) == 4:
    #         return '3'
    #     elif len(ecode) == 5:
    #         return '4'
    #     elif len(ecode) == 6:
    #         return '5'
    #     elif len(ecode) == 7:
    #         return '6'
    #
    # # Create new 'EUNIS_Level' column which indicates the numerical value of the EUNIS level by passing the fact_tbl to
    # # the eunis_lvl() function.
    # maresa_annex_merge['EUNIS_Level'] = maresa_annex_merge.apply(lambda row: eunis_lvl(row), axis=1)

    ####################################################################################################################

    # Level 6 to 5 aggregation (sub-setting)

    # Remove any unwanted trailing whitespace from all elements to be combined in together() function
    maresa_annex_merge['Annex I Habitat'] = maresa_annex_merge['Annex I Habitat'].str.strip()
    maresa_annex_merge['Annex I sub-type'] = maresa_annex_merge['Annex I sub-type'].str.strip()
    maresa_annex_merge['Depth zone'] = maresa_annex_merge['Depth zone'].str.strip()

    # Create function which takes the Annex I Feature/SubFeature columns, and combines both entries into a single column
    # This enables the data to be grouped and aggregated using the .groupby() function (does not support multiple
    # simultaneous aggregations)
    def together(row):
        # Pull in data from both columns of interest
        ann1 = row['Annex I Habitat']
        sub_f = row['Annex I sub-type']
        depth = row['Depth zone']
        # Return a string of both individual targets combined by a ' - ' symbol
        return str(str(ann1) + ' - ' + str(sub_f) + ' - ' + str(depth))

    # Run the together() function to combine Feature/SubFeature data into a single 'FeatureSubFeatureDepth' column to be
    # aggregated
    maresa_annex_merge['FeatureSubFeatureDepth'] = maresa_annex_merge.apply(lambda row: together(row), axis=1)

    # Group all L6 data by Sensitivity values
    maresa_annex_agg = maresa_annex_merge.groupby([
        'Pressure', 'FeatureSubFeatureDepth'
    ])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    maresa_annex_agg = pd.DataFrame(maresa_annex_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    maresa_annex_agg = maresa_annex_agg.reset_index(inplace=False)

    # Define function to count the number of assessment values
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

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    maresa_annex_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
                  'Unknown']] = maresa_annex_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    maresa_annex_agg['Count_High'] = maresa_annex_agg['High']
    maresa_annex_agg['Count_Medium'] = maresa_annex_agg['Medium']
    maresa_annex_agg['Count_Low'] = maresa_annex_agg['Low']
    maresa_annex_agg['Count_NotSensitive'] = maresa_annex_agg['Not sensitive']
    maresa_annex_agg['Count_NotRel'] = maresa_annex_agg['Not relevant']
    maresa_annex_agg['Count_NoEvidence'] = maresa_annex_agg['No evidence']
    maresa_annex_agg['Count_NotAssessed'] = maresa_annex_agg['Not assessed']
    maresa_annex_agg['Count_Unknown'] = maresa_annex_agg['Unknown']

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
        maresa_annex_agg[eachCol] = maresa_annex_agg[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Section XX: Level 6 (aggregation)
    #
    # # Aggregate data using the functions defined in section 2b
    # # Define function to create a sensitivity value and return a string instance
    # # Function Title: create_sensitivity
    # def create_sensitivity(df):
    #     """Series of conditional statements which return a string value of all assessment values
    #     contained within individual columns"""
    #     # Create object oriented variable for each column of data from DataFrame (assessed only)
    #     high = df['High']
    #     med = df['Medium']
    #     low = df['Low']
    #     nsens = df['Not sensitive']
    #     # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
    #     nrel = df['Not relevant']
    #     nev = df['No evidence']
    #     n_ass = df['Not assessed']
    #     un = df['Unknown']
    #
    #     # Create empty list for all string values to be appended into - this will be assigned to each field when data
    #     # are iterated through using the lambdas function which follows immediately after this function
    #     value = []
    #     # Create series of conditional statements to append string values into the empty list ('value') if conditional
    #     # statements are fulfilled
    #     if 'High' in high:
    #         h = 'High'
    #         value.append(h)
    #     if 'Medium' in med:
    #         m = 'Medium'
    #         value.append(m)
    #     if 'Low' in low:
    #         lo = 'Low'
    #         value.append(lo)
    #     if 'Not sensitive' in nsens:
    #         ns = 'Not sensitive'
    #         value.append(ns)
    #     if 'Not relevant' in nrel:
    #         nr = 'Not relevant'
    #         value.append(nr)
    #     if 'No evidence' in nev:
    #         ne = 'No evidence'
    #         value.append(ne)
    #     if 'Not assessed' in n_ass:
    #         nass = 'Not assessed'
    #         value.append(nass)
    #     if 'Unknown' in un:
    #         unk = 'Unknown'
    #         value.append(unk)
    #     s = ', '.join(value)
    #     return str(s)
    #
    # # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    # maresa_annex_agg['Sensitivity'] = maresa_annex_agg.apply(lambda df: create_sensitivity(df), axis=1)

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
            if 'Unknown' in un:
                unk = 'Unknown'
                value.append(unk)

        s = ', '.join(value)
        return str(s)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    maresa_annex_agg['AggregatedSensitivity'] = maresa_annex_agg.apply(lambda df: final_sensitivity(df), axis=1)

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
        s = ', '.join(set(value))
        return str(s)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    maresa_annex_agg['AssessedCount'] = maresa_annex_agg.apply(lambda df: combine_assessedcounts(df), axis=1)

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
        s = ', '.join(set(values))
        return str(s)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    maresa_annex_agg['UnassessedCount'] = maresa_annex_agg.apply(lambda df: combine_unassessedcounts(df), axis=1)

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

        return round(total_ass / total, 3) if total else 0

    # Use lambda function to apply create_confidence() function to the DataFrame
    maresa_annex_agg['AggregationConfidenceValue'] = maresa_annex_agg.apply(lambda df: create_confidence(df), axis=1)

    # Function Title: categorise_confidence
    def categorise_confidence(df, column):
        """Partition and categorise confidence values by quantile intervals"""
        if column == 'AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'

    # Create categories for confidence values: EUNIS Level 2
    maresa_annex_agg['AggregationConfidenceScore'] = maresa_annex_agg.apply(
        lambda df: categorise_confidence(df, 'AggregationConfidenceValue'), axis=1)

    # Refine DF to retain columns of interest
    maresa_annex_agg = maresa_annex_agg[
        ['Pressure', 'FeatureSubFeatureDepth', 'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
         'AggregationConfidenceValue', 'AggregationConfidenceScore']
    ]

    # # Remove child biotopes A5.7111 + A5.7112 from aggregation data due to prioritisation of Level 4 assessments.
    # L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7111']
    # L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7112']

    ####################################################################################################################

    # Split the FeatureSubFeatureDepth column back into the individual columns once aggregated by unique combination of
    # Feature and Sub-Feature.

    def str_split(row, str_interval):
        # Import the target column into the local scope of the function
        target_col = row['FeatureSubFeatureDepth']
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
        # Split the target string to get the Depth using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'Depth':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the second of the two list items
            return str(result[2])

    # Run the str_split() function to return the combined Feature data back into two separate columns
    maresa_annex_agg['Annex I Habitat'] = maresa_annex_agg.apply(lambda row: str_split(row, 'Feature'), axis=1)

    # Run the str_split() function to return the combined SubFeature data back into two separate columns
    maresa_annex_agg['Annex I sub-type'] = maresa_annex_agg.apply(lambda row: str_split(row, 'SubFeature'),
                                                                                axis=1)
    # Run the str_split() function to return the combined Depth data back into two separate columns
    maresa_annex_agg['Depth zone'] = maresa_annex_agg.apply(
        lambda row: str_split(row, 'Depth'),
        axis=1)

    # Rearrange columns correctly into DataFrame schema
    maresa_annex_agg = maresa_annex_agg[[
        'Pressure', 'Annex I Habitat', 'Annex I sub-type',
        'Depth zone', 'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
         'AggregationConfidenceValue', 'AggregationConfidenceScore'
    ]]

    # Remove all data from the DF which is flagged to be NaN within the 'Annex I Habitat' column
    maresa_annex_agg = maresa_annex_agg[maresa_annex_agg['Annex I Habitat'] != 'nan']

    # Export data export

    # Define folder file path to be saved into
    outpath = r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation"
    # Define file name to save, categorised by date
    filename = "FeatureLevelAggregationDepth_V6L5_" + (time.strftime("%Y%m%d") + ".csv")
    # Run the output DF.to_csv method
    maresa_annex_agg.to_csv(outpath + "\\" + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print("The 'Annex1SubTypeAggregationDepth' script took " + str(round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
          r'This has been saved as an output at the following filepath: Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\DepthAggregation')

# Run function / entire script - to be called from AggregationExecution script
main()
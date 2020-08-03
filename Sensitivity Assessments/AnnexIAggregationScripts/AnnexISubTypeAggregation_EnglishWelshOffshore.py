########################################################################################################################

# Title: Annex I Sub Type Aggregation English and Welsh Offshore

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
    annex1 = pd.read_excel(r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\Priority2_AnnexI_EnglandWalesOffshore\EnglishWelshOffshore_AnnexI.xlsx", 'BiotopesForAgg_Regions_subfeat', dtype=str)

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
        'Subregion', 'JNCC_Code', 'Annex I habitat', 'Annex I sub feature type', 'Classification level', 'EUNIS code',
        'EUNIS_Code', 'Biotope name', 'JNCC code', 'JNCC name', 'Pressure', 'Resilience', 'Resistance', 'Sensitivity'
    ]]

    ####################################################################################################################

    # Remove any unwanted trailing whitespace from all elements to be combined in together() function
    maresa_annex_merge['Subregion'] = maresa_annex_merge['Subregion'].str.strip()
    maresa_annex_merge['Annex I habitat'] = maresa_annex_merge['Annex I habitat'].str.strip()
    maresa_annex_merge['Annex I sub feature type'] = maresa_annex_merge['Annex I sub feature type'].str.strip()

    # Create function which takes the Annex I Feature/SubFeature columns, and combines both entries into a single column
    # This enables the data to be grouped and aggregated using the .groupby() function (does not support multiple
    # simultaneous aggregations)
    def together(row):
        # Pull in data from both columns of interest
        subb_r = row['Subregion']
        ann1 = row['Annex I habitat']
        sub_f = row['Annex I sub feature type']
        # Return a string of both individual targets combined by a ' - ' symbol
        return str(subb_r) + ' - ' + str(str(ann1) + ' - ' + str(sub_f))

    # Run the together() function to combine Feature/SubFeature data into a single 'SubregionFeatureSubFeature' column to be
    # aggregated
    maresa_annex_merge['SubregionFeatureSubFeature'] = maresa_annex_merge.apply(lambda row: together(row), axis=1)

    # Group all L6 data by Sensitivity values
    maresa_annex_agg = maresa_annex_merge.groupby([
        'Pressure', 'SubregionFeatureSubFeature'
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
        ['Pressure', 'SubregionFeatureSubFeature', 'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
         'AggregationConfidenceValue', 'AggregationConfidenceScore']
    ]

    # # Remove child biotopes A5.7111 + A5.7112 from aggregation data due to prioritisation of Level 4 assessments.
    # L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7111']
    # L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7112']

    ####################################################################################################################

    # Split the SubregionFeatureSubFeature column back into the individual columns once aggregated by unique combination of
    # Feature and Sub-Feature.

    def str_split(row, str_interval):
        # Import the target column into the local scope of the function
        target_col = row['SubregionFeatureSubFeature']
        # Split the target string to get the Feature using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'Subregion':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the first of the two list items
            return str(result[0])
        if str_interval == 'Feature':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the first of the two list items
            return str(result[1])
        # Split the target string to get the SubFeature using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'SubFeature':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the second of the two list items
            return str(result[2])

    # Run the str_split() function to return the combined Feature data back into two separate columns
    maresa_annex_agg['Bioregion'] = maresa_annex_agg.apply(lambda row: str_split(row, 'Subregion'), axis=1)

    # Run the str_split() function to return the combined Feature data back into two separate columns
    maresa_annex_agg['Annex I Habitat'] = maresa_annex_agg.apply(lambda row: str_split(row, 'Feature'), axis=1)

    # Run the str_split() function to return the combined SubFeature data back into two separate columns
    maresa_annex_agg['Annex I sub-type'] = maresa_annex_agg.apply(lambda row: str_split(row, 'SubFeature'),
                                                                                axis=1)

    # Rearrange columns correctly into DataFrame schema
    maresa_annex_agg = maresa_annex_agg[[
        'Pressure', 'Bioregion', 'Annex I Habitat', 'Annex I sub-type',
        'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
         'AggregationConfidenceValue', 'AggregationConfidenceScore'
    ]]

    # Remove all data from the DF which is flagged to be NaN within the 'Annex I Habitat' column
    maresa_annex_agg = maresa_annex_agg[maresa_annex_agg['Annex I Habitat'] != 'nan']

    # Export data export

    # Define folder file path to be saved into
    outpath = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\Priority2_AnnexI_EnglandWalesOffshore"
    # Define file name to save, categorised by date
    filename = "FeatureLevelAggregationEnglishWelshOffshore_" + (time.strftime("%Y%m%d") + ".csv")
    # Run the output DF.to_csv method
    maresa_annex_agg.to_csv(outpath + "\\" + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print("The 'Annex1SubTypeAggregationEnglishWelshOffshore' script took " + str(
        round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
          r"This has been saved as an output at the following filepath: \\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\Priority2_AnnexI_EnglandWalesOffshore\EnglishWelshOffshoreAggregationOutputs")


# Run function / entire script - to be called from AggregationExecution script
main()
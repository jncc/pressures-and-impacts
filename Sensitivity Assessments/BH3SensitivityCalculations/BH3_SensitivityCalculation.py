########################################################################################################################

# Title: BH3 Sensitivity Calculation

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    ONGOING WORK IN PROGRESS
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                           BH3 Sensitivity Calculations:                                              #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import os
import time
import pandas as pd


# Create function to execute main script
def main():

    ####################################################################################################################
    start = time.process_time()

    # Define functions used within either version of the executed code

    # Define function which takes a range and returns the most precautionary value
    def precautionary_value(row, assessment_type, column):
        # Process the most precautionary value from all resistance scores
        if assessment_type == 'Resistance':
            res = str(row[column])
            if 'None' in res:
                return 'None'
            elif 'None' not in res and 'Low' in res:
                return 'Low'
            elif 'None' not in res and 'Low' not in res and 'Medium' in res:
                return 'Medium'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' in res:
                return 'High'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' in res:
                return 'Not relevant'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' not in res and 'Not assessed' in res:
                return 'Not assessed'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' not in res and 'Not assessed' not in res and 'Unknown' in res:
                return 'Unknown'

        # Process the most precautionary value from all resistance scores
        # NOTE: precautionary values for resilience do not currently account for 'Very High' as this value is not
        # used within JNCC data
        elif assessment_type == 'Resilience':
            resil = str(row[column])
            if 'Very Low' in resil:
                return 'Very Low'
            elif 'Very Low' not in resil and 'Low' in resil:
                return 'Low'
            elif 'Very Low' not in resil and 'Low' not in resil and 'Medium' in resil:
                return 'Medium'
            elif 'Very Low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' in resil:
                return 'High'
            elif 'Very Low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' in resil:
                return 'Not relevant'
            elif 'Very Low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' not in resil and 'Not assessed' in resil:
                return 'Not assessed'
            elif 'Very Low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' not in resil and 'Not assessed' not in resil and 'Unknown' in resil:
                return 'Unknown'

    # Define function to iterate through the DF and assign a BH3 sensitivity value based on the resistance and
    # resilience score
    def bh3_calc(row, eunislvl):
        # Run calculations for EUNIS level 2 aggregations
        if eunislvl == 2:
            # Conduct calculation if the value is 'None'
            if precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very Low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'High'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2

            # Conduct calculation if the value is 'Not assessed'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not assessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not assessed':
                return -1

            # Conduct calculation if the value is 'No evidence'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not evidence' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not evidence':
                return -1

            # Conduct calculation if the value is 'Unknown'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unknown' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unknown':
                return -1

        # Run calculations for EUNIS level 3 aggregations
        elif eunislvl == 3:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very Low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2

            # Conduct calculation if the value is 'Not assessed'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not assessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not assessed':
                return -1

            # Conduct calculation if the value is 'No evidence'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not evidence' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not evidence':
                return -1

            # Conduct calculation if the value is 'Unknown'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unknown' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unknown':
                return -1

        # Run calculations for EUNIS level 4 aggregations
        elif eunislvl == 4:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very Low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2

            # Conduct calculation if the value is 'Not assessed'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not assessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not assessed':
                return -1

            # Conduct calculation if the value is 'No evidence'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not evidence' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not evidence':
                return -1

            # Conduct calculation if the value is 'Unknown'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unknown' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unknown':
                return -1

        # Run calculations for EUNIS level 5 aggregations
        elif eunislvl == 5:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very Low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2

            # Conduct calculation if the value is 'Not assessed'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not assessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not assessed':
                return -1

            # Conduct calculation if the value is 'No evidence'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not evidence' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not evidence':
                return -1

            # Conduct calculation if the value is 'Unknown'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unknown' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unknown':
                return -1

        # Run calculations for EUNIS level 5 aggregations
        elif eunislvl == 6:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very Low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 3

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 2

            # Conduct calculation if the value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2

            # Conduct calculation if the value is 'Not assessed'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not assessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not assessed':
                return -1

            # Conduct calculation if the value is 'No evidence'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not evidence' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not evidence':
                return -1

            # Conduct calculation if the value is 'Unknown'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unknown' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unknown':
                return -1

    ####################################################################################################################

    # Import aggregated resistance data

    # Run short analysis to identify the most recently created iteration of the Resistance Aggregation and read this in
    # for the BH3 process

    # Define a directory to be searched
    res_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resistance"
    # Set this as the working directory
    os.chdir(res_dir)
    # Read in all files within this target directory
    res_files = filter(os.path.isfile, os.listdir(res_dir))
    # Add full filepaths to the identified files within said directory
    res_files = [os.path.join(res_dir, f) for f in res_files]
    # Sort all files read in by the most recently edited first
    res_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the bioregions object containing the updated outputs from the Bioregions 2017 Contract - read the last
    # edited file
    resistance_masterframeOFF = pd.read_csv(res_files[0], dtype=str)

    ############################################################

    # Create an object variable storing the date of the bioregions version used - this is entered into the aggregation
    # output file name for QC purposes.
    # Create an object storing a string of the loaded bioregions data
    loaded_resistance = res_files[0]
    # String split the loaded file to provide the date stored within the file name
    res_date = str(loaded_resistance).split('\\')[-1]
    # Re-split the string to remove the .csv file extension
    res_version = str(res_date).split('.')[0]
    # Re-split the file name to only retain the date of creation
    res_version = str(res_version).split('_')[-1]
    # Create an abreviated version of the filename with the date
    res_version = 'ResAgg' + str(res_version)

    ############################################################
    # Run short analysis to identify the most recently created iteration of the Resilience Aggregation and read this in
    # for the BH3 process

    # Define a directory to be searched
    resil_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resilience"
    # Set this as the working directory
    os.chdir(resil_dir)
    # Read in all files within this target directory
    resil_files = filter(os.path.isfile, os.listdir(resil_dir))
    # Add full filepaths to the identified files within said directory
    resil_files = [os.path.join(resil_dir, f) for f in resil_files]
    # Sort all files read in by the most recently edited first
    resil_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the bioregions object containing the updated outputs from the Bioregions 2017 Contract - read the last
    # edited file
    resilience_masterframeOFF = pd.read_csv(resil_files[0], dtype=str)

    ############################################################

    # Create an object variable storing the date of the bioregions version used - this is entered into the aggregation
    # output file name for QC purposes.
    # Create an object storing a string of the loaded bioregions data
    loaded_resilience = resil_files[0]
    # String split the loaded file to provide the date stored within the file name
    resil_date = str(loaded_resilience).split('\\')[-1]
    # Re-split the string to remove the .csv file extension
    resil_version = str(resil_date).split('.')[0]
    # Re-split the file name to only retain the date of creation
    resil_version = str(resil_version).split('_')[-1]
    # Create an abreviated version of the filename with the date
    resil_version = 'ResilAgg' + str(resil_version)

    ############################################################

    # Subset the resistance_masterframe to only retain the required values for the BH3 sensitivity calculation
    # process
    resistance_masterframeOFF = resistance_masterframeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_AggregationConfidenceValue',
        'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResistance', 'L3_AggregationConfidenceValue',
        'L3_AggregationConfidenceScore', 'Level_4', 'L4_FinalResistance', 'L4_AggregationConfidenceValue',
        'L4_AggregationConfidenceScore', 'Level_5', 'L5_FinalResistance', 'L5_AggregationConfidenceValue',
        'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalResistance', 'L6_AggregationConfidenceValue',
        'L6_AggregationConfidenceScore'
    ]]

    # Subset the resilience_masterframe to only retain the required values for the BH3 sensitivity calculation
    # process
    resilience_masterframeOFF = resilience_masterframeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResilience', 'L2_AggregationConfidenceValue',
        'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResilience', 'L3_AggregationConfidenceValue',
        'L3_AggregationConfidenceScore', 'Level_4', 'L4_AggregationConfidenceValue',
        'L4_AggregationConfidenceScore', 'L4_FinalResilience', 'Level_5', 'L5_FinalResilience',
        'L5_AggregationConfidenceValue', 'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalResilience',
        'L6_AggregationConfidenceValue', 'L6_AggregationConfidenceScore'
    ]]

    # Create a merged single DF which stores the relevant resistance and resilience information for each
    # biotope/pressure interaction
    res_resil_mergeOFF = pd.merge(resistance_masterframeOFF, resilience_masterframeOFF, how='outer', on=[
        'Pressure', 'SubregionName', 'Level_2', 'Level_3', 'Level_4', 'Level_5', 'Level_6'
    ])

    # Create code which corrects the erroneous data supplied within the MarESA extract prior to running the BH3
    # calculations. This is completed through DF .loc() with multiple conditions - where EUNIS code is X, and pressure
    # is Y, resistance or resilience value becomes Z

    # The errors to be fixed are:

    # A5.332 - Introduction of light, resistance should be ‘high’, not ‘no evidence’
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.332') &
        (res_resil_mergeOFF['Pressure'] == 'Introduction of light') &
        (res_resil_mergeOFF['L5_FinalResistance'] == 'No evidence'),
        'L5_FinalResistance'] = 'High'

    # A1.111 - Electromagnetic changes, resistance should be ‘no evidence’, not high.
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A1.111') &
        (res_resil_mergeOFF['Pressure'] == 'Electromagnetic changes') &
        (res_resil_mergeOFF['L5_FinalResistance'] == 'High'),
        'L5_FinalResistance'] = 'No evidence'

    # A3.223 – Physical change (to another sediment type), resilience should be ‘Not Relevant’ instead of Very Low.
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A3.223') &
        (res_resil_mergeOFF['Pressure'] == 'Physical change (to another sediment type)') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'Very Low'),
        'L5_FinalResilience'] = 'Not relevant'

    # A5.222 - Salinity increase, resilience should be ‘high’ not ‘No evidence’ and overall sensitivity should be ‘Low’
    # instead of ‘no evidence’
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.222') &
        (res_resil_mergeOFF['Pressure'] == 'Salinity increase') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'No evidence'),
        'L5_FinalResilience'] = 'High'

    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.222') &
        (res_resil_mergeOFF['Pressure'] == 'Salinity increase') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'No evidence'),
        'L5_FinalSensitivity'] = 'Low'

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L2' column
    res_resil_mergeOFF['BH3_L2'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 2), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L3' column
    res_resil_mergeOFF['BH3_L3'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 3), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L4' column
    res_resil_mergeOFF['BH3_L4'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 4), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L5' column
    res_resil_mergeOFF['BH3_L5'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 5), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L5' column
    res_resil_mergeOFF['BH3_L6'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 6), axis=1)

    # Fill all blank / na values with 'Cannot complete BH3 calculation'
    res_resil_mergeOFF['BH3_L2'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L3'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L4'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L5'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L6'].fillna('Cannot complete BH3 calculation', inplace=True)

    # Rearrange the merged DF to represent a coherent schema and structure
    res_resil_mergeOFF = res_resil_mergeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_FinalResilience', 'BH3_L2', 'Level_3',
        'L3_FinalResistance', 'L3_FinalResilience', 'BH3_L3', 'Level_4', 'L4_FinalResistance', 'L4_FinalResilience',
        'BH3_L4', 'Level_5', 'L5_FinalResistance', 'L5_FinalResilience', 'BH3_L6', 'Level_6', 'L6_FinalResistance',
        'L6_FinalResilience', 'BH3_L6'
    ]]

    # Export the output res_resil_mergeOFF DF with all computed BH3 values

    # Define folder file path to be saved into
    outpath = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\BH3Calculations"
    # Define file name to save, categorised by date
    filename = "BH3_OffshoreSensitivityCalculation_" + (time.strftime("%Y%m%d") + '_' + str(res_version) + '_'
                                                        + str(resil_version) + ".csv")
    # Run the output DF.to_csv method
    res_resil_mergeOFF.to_csv(outpath + "\\" + filename, sep=',')

    ####################################################################################################################

    # INSHORE / OFFSHORE - NOT CURRENTLY COMPLETED - DO NOT RUN LM: 25/09/2019

    # # Import all data from the processed resistance and resilience aggregations. These are the masterframes exported
    # # through the aggregation process
    #
    # # Import aggregated resistance data
    # resistance_masterframe = \
    #     pd.read_csv(
    #         'J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\ResistanceInshoreOffshore.csv'
    #     )
    #
    # # Import aggregated resilience data
    # resilience_masterframe = \
    #     pd.read_csv(
    #         'J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\ResilienceInshoreOffshore.csv'
    #     )
    #
    # # Subset the resistance_masterframe to only retain the required values for the BH3 sensitivity calculation
    # # process
    # resistance_masterframe = resistance_masterframe[[
    #     'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_AggregationConfidenceValue',
    #     'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResistance', 'L3_AggregationConfidenceValue',
    #     'L3_AggregationConfidenceScore', 'Level_4', 'L4_FinalResistance', 'L4_AggregationConfidenceValue',
    #     'L4_AggregationConfidenceScore', 'Level_5', 'L5_FinalResistance', 'L5_AggregationConfidenceValue',
    #     'L5_AggregationConfidenceScore'
    # ]]
    #
    # # Subset the resilience_masterframe to only retain the required values for the BH3 sensitivity calculation
    # # process
    # resilience_masterframe = resilience_masterframe[[
    #     'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResilience', 'L2_AggregationConfidenceValue',
    #     'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResilience', 'L3_AggregationConfidenceValue',
    #     'L3_AggregationConfidenceScore', 'Level_4', 'L4_AggregationConfidenceValue',
    #     'L4_AggregationConfidenceScore', 'L4_FinalResilience', 'Level_5', 'L5_FinalResilience',
    #     'L5_AggregationConfidenceValue', 'L5_AggregationConfidenceScore'
    # ]]
    #
    # # Create a merged single DF which stores the relevant resistance and resilience information for each
    # # biotope/pressure interaction
    # res_resil_merge = pd.merge(resistance_masterframe, resilience_masterframe, how='outer', on=[
    #     'Pressure', 'SubregionName', 'Level_2', 'Level_3', 'Level_4', 'Level_5'
    # ])
    #
    # # Create code which corrects the erroneous data supplied within the MarESA extract prior to running the BH3
    # # calculations. This is completed through DF .loc() with multiple conditions - where EUNIS code is X, and pressure
    # # is Y, resistance or resilience value becomes Z
    #
    # # The errors to be fixed are:
    #
    # # A5.332 - Introduction of light, resistance should be ‘high’, not ‘no evidence’
    # res_resil_merge.loc[
    #     (res_resil_merge['Level_5'] == 'A5.332') &
    #     (res_resil_merge['Pressure'] == 'Introduction of light') &
    #     (res_resil_merge['L5_FinalResistance'] == 'No evidence'),
    #     'L5_FinalResistance'] = 'High'
    #
    # # A1.111 - Electromagnetic changes, resistance should be ‘no evidence’, not high.
    # res_resil_merge.loc[
    #     (res_resil_merge['Level_5'] == 'A1.111') &
    #     (res_resil_merge['Pressure'] == 'Electromagnetic changes') &
    #     (res_resil_merge['L5_FinalResistance'] == 'High'),
    #     'L5_FinalResistance'] = 'No evidence'
    #
    # # A3.223 – Physical change (to another sediment type), resilience should be ‘Not Relevant’ instead of Very Low.
    # res_resil_merge.loc[
    #     (res_resil_merge['Level_5'] == 'A3.223') &
    #     (res_resil_merge['Pressure'] == 'Physical change (to another sediment type)') &
    #     (res_resil_merge['L5_FinalResilience'] == 'Very Low'),
    #     'L5_FinalResilience'] = 'Not relevant'
    #
    # # A5.222 - Salinity increase, resilience should be ‘high’ not ‘No evidence’ and overall sensitivity should be ‘Low’
    # # instead of ‘no evidence’
    # res_resil_merge.loc[
    #     (res_resil_merge['Level_5'] == 'A5.222') &
    #     (res_resil_merge['Pressure'] == 'Salinity increase') &
    #     (res_resil_merge['L5_FinalResilience'] == 'No evidence'),
    #     'L5_FinalResilience'] = 'High'
    #
    # res_resil_merge.loc[
    #     (res_resil_merge['Level_5'] == 'A5.222') &
    #     (res_resil_merge['Pressure'] == 'Salinity increase') &
    #     (res_resil_merge['L5_FinalResilience'] == 'No evidence'),
    #     'L5_FinalSensitivity'] = 'Low'
    #
    # # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # # resulting value to the new 'BH3_L2' column
    # res_resil_merge['BH3_L2'] = res_resil_merge.apply(lambda row: bh3_calc(row, 2), axis=1)
    #
    # # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # # resulting value to the new 'BH3_L3' column
    # res_resil_merge['BH3_L3'] = res_resil_merge.apply(lambda row: bh3_calc(row, 3), axis=1)
    #
    # # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # # resulting value to the new 'BH3_L4' column
    # res_resil_merge['BH3_L4'] = res_resil_merge.apply(lambda row: bh3_calc(row, 4), axis=1)
    #
    # # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # # resulting value to the new 'BH3_L5' column
    # res_resil_merge['BH3_L5'] = res_resil_merge.apply(lambda row: bh3_calc(row, 5), axis=1)
    #
    # # Fill all blank / na values with 'Cannot complete BH3 calculation'
    # res_resil_merge['BH3_L2'].fillna('Cannot complete BH3 calculation', inplace=True)
    # res_resil_merge['BH3_L3'].fillna('Cannot complete BH3 calculation', inplace=True)
    # res_resil_merge['BH3_L4'].fillna('Cannot complete BH3 calculation', inplace=True)
    # res_resil_merge['BH3_L5'].fillna('Cannot complete BH3 calculation', inplace=True)
    #
    # # Rearrange the merged DF to represent a coherent schema and structure
    # res_resil_merge = res_resil_merge[[
    #     'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_FinalResilience', 'BH3_L2', 'Level_3',
    #     'L3_FinalResistance', 'L3_FinalResilience', 'BH3_L3', 'Level_4', 'L4_FinalResistance', 'L4_FinalResilience',
    #     'BH3_L4', 'Level_5', 'L5_FinalResistance', 'L5_FinalResilience', 'BH3_L5'
    # ]]
    #
    # # Export the output DF with all computed BH3 values
    # res_resil_merge.\
    #     to_csv(
    #     r'J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\BH3_SensitivityCalculationInshoreOffshore.csv', sep=','
    # )

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print("The 'BH3_SensitivityCalculation Offshore' script took " + str(
        round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
          'These have been saved as .CSV outputs at the following filepath: J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main')


# Run code as main
main()
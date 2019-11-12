########################################################################################################################

# Title: Bioregions Inshore Biotopes Removal

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    Read in all data from Bioregions Automation process and remove all erroneous inshore data.
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk

########################################################################################################################

#                                                Introduction & Setup                                                  #

########################################################################################################################

# Import all required libraries for the data manipulation process
import pandas as pd

# Read in data outputted from Bioregions Automation Script
Bioregions = pd.read_csv(r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Contracts\C16-0257-105 Biogeographical Regional Contract\BioregionsAutomation_2019\Output20190825.csv")

# Define list of biotopes to exclude from DF
biotopes = [
    'A1.1131', 'A1.123', 'A1.311', 'A1.3122', 'A1.3141', 'A1.3142', 'A1.3151', 'A1.412', 'A1.421', 'A2.611', 'A3.1112',
    'A5.5211', 'A5.53', 'B3.111', 'B3.1132'
]

# Create new DF which in which the listed biotopes are not within the EUNIS_code column of the DF
BioregionsRefined = Bioregions.loc[~Bioregions['EUNIS_code'].isin(biotopes)]

# Export the new DF as a CSV to the same folder file path
BioregionsRefined.to_csv(r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Contracts\C16-0257-105 Biogeographical Regional Contract\BioregionsAutomation_2019\Output_InshoreBiotopesRemoved.csv", sep=',')

















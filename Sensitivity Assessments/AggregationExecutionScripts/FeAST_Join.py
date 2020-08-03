########################################################################################################################

# Title: FeAST Join

# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk


########################################################################################################################

#                                             Join data together for FeAST                                             #

########################################################################################################################

# Import all libraries required to complete processes
import pandas as pd
import os

# Read the first DF - MarESA Special Extract
maresa_sp = pd.read_excel(r'\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\MarLIN\Deliverables\FeAST_SpecialExtract\MarESA-special-extract-FeAST-Annex1-2020-02-14_Final.xlsm')

# Read in the second DF - Annex 1 Sub-types
annexI = pd.read_excel(r'\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\AnnexI_sub_types_all_v6.xlsx', 'L5_BiotopesForAgg')

# Full join all data by EUNIS Code
fulljoin = pd.merge(maresa_sp, annexI, left_on='EUNIS_Code', right_on='EUNIS code', how='outer')

# Refine DF to only retain required columns
fulljoin = fulljoin[
    [
        'EUNIS_Code', 'Pressure', 'Resistance', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE', 'Resilience',
        'ResilienceQoE', 'ResilienceAoE', 'ResilienceDoE', 'Sensitivity', 'SensitivityQoE', 'SensitivityAoE',
        'SensitivityDoE', 'Evidence', 'url', 'Evidence_cleaned', 'Annex I Habitat', 'Annex I sub-type', 'Depth zone',
        'Biotope name'
    ]
]

# Export DF
fulljoin.to_csv(r'\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\TrialOutputs\FeAST_SpecialExtractBedrockStony_.csv', sep=',')


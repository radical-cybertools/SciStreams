from SciAnalysis.analyses.XSAnalysis.Data import MasterMask
from config import MASKDIR

master_mask_name = "pilatus300_mastermask.npz"
master_mask_filename = MASKDIR + "/" + master_mask_name

master_mask = MasterMask(datafile=master_mask_filename)

submask = master_mask.generate((900,900), (300,400))
submask = submask.compute()



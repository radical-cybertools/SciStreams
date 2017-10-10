# GISAXS with Pilatus300
uids = [
"f26af936-50ce-43f2-a1b5-6c9a06d3ba5a",
#"8bc0b0bd-dabd-4746-8fab-7a6a9b6d82a7",
#"c30ad46c-f630-4ed4-8e51-9879b4eedc97",
#"c869f360-391a-437e-84c7-6a8d23b148fa",
#"161caf82-d69e-48c7-95ff-91f8c7c637e2",
#"53610210-48f8-486f-9310-5e52cc8d746a",
#"93f689b2-766a-44aa-9218-a09b45de1f94",
#"295d1075-6cde-48e0-a5a4-e2a99eb027b3",
#"ffa28181-b10d-4da1-b2ec-611dfe0dc38b",
#"0e974f1f-5dec-47a8-b797-188b048900d7",
#"ce99d70e-7fe9-4dd9-b186-d86f1254fc4d",
#"4895a0a3-e045-497c-b5b9-e46301b3a527",
#"08871842-a6a4-4c0b-95a2-a2ff8cc29201",
#"6b02d0f8-c868-42b2-9be7-32c3eee9af85",
#"9ae8c4a1-19e9-4e21-b004-7ba8ea5bc3c9",
#"bf155e4f-230a-48c2-87dc-eeb703ce543d",
#"09429ac7-3b42-4e77-af56-209b091c2d38",
#"380f5028-3280-4bab-9424-ca284d746196",
#"ba2be44b-e46a-4be7-8494-508a688bdd81",
#"d338e423-1063-450f-b01e-ba14befe93c6",
#"1338b020-ef96-463e-9af3-206262e41cc8",
#"d3dbbfd1-cf44-494c-b677-c0c8ca456a0e",
#"9eae2a51-e041-43d5-b000-ffe9710dadc4",
#"392b1af0-1f22-4c1a-bae2-550fa5ec8251",
#"4a341909-d8fe-44b5-b3ee-0f6be3fb9603",
]


detector_key = "pilatus2M_image"

from SciStreams.examples.mask_creation import start_mask, resume
# run with start_mask(uids_gisaxs_pilatus300, 'pilatus300_image')
start_mask(uids, detector_key)
from SciStreams.examples.mask_creation import msk
msk.set_clim(0,10)

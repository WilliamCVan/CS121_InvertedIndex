import json

masterFilePath = "C:\\Anaconda3\\envs\\Projects\\developer\\DEV"
listOne = [
    # Top 5 Docs for 'cristina lopes'
(f'{masterFilePath}\\www_ics_uci_edu\\8d32fb15342504573e440e19d69610e2e904d5068dff2f88b10565b27825c462.json', 4747.206577271334),
(f'{masterFilePath}\\www_informatics_uci_edu\\5c1b9599d18d8b69c2548218633f7ec44982aeec0be06285f3fb3bf7e365da6f.json', 4747.206577271334),
(f'{masterFilePath}\\www_informatics_uci_edu\\294cf249f6225e2b2651242ac70a9e883cdaaf88e5dbb30e4a9230639aa9bab0.json', 4747.206577271334),
(f'{masterFilePath}\\www_informatics_uci_edu\\4cd4d486d936cbfd03c38a78d0bd59f59cbe44477c80884fedf8b9cb85a29faf.json', 4747.206577271334),
(f'{masterFilePath}\\www_ics_uci_edu\\138bb3f52292479ba91ef93eab3dac4ecd7f00e59409c3e6f9b96eb70ac37663.json', 4747.206577271334)
]

listTwo = [
#Top 5 Docs for 'machine learning'
(f'{masterFilePath}\\grape_ics_uci_edu\\549fc2fc43e4c8f2716057e776beeeb89b559f23d5abdfdee865fcfec492cd8a.json', 204514.79341896693),
(f'{masterFilePath}\\www_ics_uci_edu\\0c77fd9f5598d7adc2215bc93d9d6d62b6b544df7ffc143d46826e1075c2dc3b.json', 204514.79341896693),
(f'{masterFilePath}\\grape_ics_uci_edu\\5c5d24cf679f95d044ca2fe9b028ef63ea6a97d0259790119619c601479a1ec8.json', 204514.79341896693),
(f'{masterFilePath}\\www_ics_uci_edu\\bb1e4728bd116b59742f0b01e56660c7b7da18e7363a14556a828af546d82298.json', 204514.79341896693),
(f'{masterFilePath}\\sli_ics_uci_edu\\492144ac4e333f55c273b0da9bf75ec072433959c34b7f0805f2c3e865588baf.json', 204514.79341896693)
]

listThree = [
# Top 5 Docs for 'ACM'
(f'{masterFilePath}\\www_ics_uci_edu\\c488adb5d0e32263d652e0d197b8e792008acf4c5e2bede5408c8fa2dace85b0.json', 132419.35619874173),
(f'{masterFilePath}\\www_ics_uci_edu\\ab4d30efe463d65e11a3d38803231d1219826c421015d86e1b9ad34214dd130c.json', 132419.35619874173),
(f'{masterFilePath}\\www_informatics_uci_edu\\7287d5a469dcf73990bc825088697cc095830511868736e91fa419aa34d2981f.json', 132419.35619874173),
(f'{masterFilePath}\\www_ics_uci_edu\\0f47128fadd03da1dac99b900b750ed2960b3a5d93a630c933300ebb5e171be4.json', 132419.35619874173),
(f'{masterFilePath}\\ngs_ics_uci_edu\\df10dc4034bfc7cbffc3a19a29976676e26980e2978d58c92358452da409baa2.json', 132419.35619874173)
]

listFour = [
# Top 5 Docs for 'master of software engineering'
(f'{masterFilePath}\\sdcl_ics_uci_edu\\847c13953a2610b69d87a7992314c9a4a7de4e47cc96f3bcda9cc7f6106cc082.json', 378944.5991026481),
(f'{masterFilePath}\\www_ics_uci_edu\\fb5d37b3f315b9e4c2a5ce4889a14d528541bd322d89e07bc4e276a18afabfb5.json', 378944.5991026481),
(f'{masterFilePath}\\swiki_ics_uci_edu\\e9a1d2f454196aff329bc884849f93cbc0bd1ca8279bf4d61db706997c933cd8.json', 378944.5991026481),
(f'{masterFilePath}\\mswe_ics_uci_edu\\345e4bd69edf6a2f141c8c21def641be10f14fecfdd73ab049b6f99c522b3b13.json', 378944.5991026481),
(f'{masterFilePath}\\www_cs_uci_edu\\9ca693c73c6639627e275d005e6c7148946887e15e969346daf9d7b8cb5b20af.json', 378944.5991026481)
]

listAllFiles = [listOne, listTwo, listThree, listFour]

listMasterURLs = []
for listSingle in listAllFiles:
    listTemp = list()
    for filePath in listSingle:
        with open(filePath[0], "r") as file:
            content = json.loads(file.read())
            url = content["url"]
            listTemp.append(url)
    listMasterURLs.append(listTemp)

with open("C:\\Anaconda3\\envs\\Projects\\CS121_InvertedIndex\\report2.txt", "w") as file:
    file.write(json.dumps(listMasterURLs))


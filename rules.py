""" Tax return rules for the parser """

dl1 = "delay* OR abandon OR eliminate OR curtail OR (scale back) OR postpone*"
dl2 = "construction OR expansion OR acquisition* OR restructuring OR project* OR research OR development OR exploration OR product* OR expenditure* OR manufactur* OR entry OR renovat* OR growth OR activities OR (capital improvement*) OR (capital spend*) OR (capital proj*) OR (commercial release) OR (business plan) OR (transmitter deployment) OR (opening restaurants)"

efl = "issuing equity securities OR expects equity securities OR through equity financing OR sources equity financing OR seek equity investments OR seek equity financings OR access equity markets OR raised equity arrangements OR undertake equity offerings OR sell common stock OR issuing common stock OR selling common stock OR use equity offerings OR offering equity securities OR planned equity offering OR seek equity offering OR raise equity offering OR equity offering would add OR additional equity offering OR considering equity offering OR seek equity financing OR pursue equity offering OR consummates equity offering OR raises equity capital OR raise equity offering OR sources equity offering"

dfl = "increased borrowings OR use line of credit OR expanded borrowings OR funded by borrowings OR additional credit lines OR incur additional indebtedness OR pursue lines of credit OR anticipates lines of credit OR through loan financing OR borrowings bond issue OR increase line of credit OR provided by credit facilities OR seek borrowing transaction OR raise borrowings OR additional bank financing OR raises debt capital OR secure line of credit OR borrowing of capital"

ppl1 = "private"
ppl2 = "placement OR placements OR sale OR sales OR offering OR offerings OR infusion OR infusions OR issued OR issuance OR financing OR financings OR funding"
ppl3 = "equity OR stock"

# Group the rules along with their name
query_rules = [("Delay list 1", dl1), ("Delay list 2", dl2), ("Equity-focused list", efl), ("Debt-focused list", dfl), ("Private placement list 1", ppl1), ("Private placement list 2", ppl2), ("Private placement list 3", ppl3)]

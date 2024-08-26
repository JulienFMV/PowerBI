let
    // Fonction pour transformer les données
    TransformData = (Source as table) as table =>
    let
        // Promouvoir les en-têtes et transformer les types de colonnes
        DataWithHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
        DataTyped = Table.TransformColumnTypes(DataWithHeaders, {{"Date", type date}}),

        // Ajout de la colonne SommeDesPrix en filtrant les lignes égales à 0
        DataWithSum = Table.AddColumn(DataTyped, "SommeDesPrix", each List.Sum(Record.FieldValues(Record.RemoveFields(_, {"Date"})))),
        DataFiltered = Table.SelectRows(DataWithSum, each [SommeDesPrix] <> 0),

        // Transformation de la table : Unpivot et manipulation des colonnes
        DataUnpivoted = Table.UnpivotOtherColumns(DataFiltered, {"Date"}, "Attribut", "Valeur"),
        DataRenamed = Table.RenameColumns(DataUnpivoted, {{"Attribut", "Produit"}, {"Valeur", "Prix_EURMWh"}}),
        DataSplit = Table.SplitColumn(DataRenamed, "Produit", Splitter.SplitTextByDelimiter(" ", QuoteStyle.Csv), {"Periode", "Annee", "Produit"}),

        // Tri des colonnes selon des critères définis
        DataTransformed = Table.TransformColumnTypes(DataSplit, {{"Periode", type text}, {"Annee", type text}, {"Produit", type text}}),

        // Ajout des colonnes pour le tri personnalisé
        DataWithPeriodOrder = Table.AddColumn(DataTransformed, "PeriodeTRI", each
            List.PositionOf(
                {"Cal", "Q1", "Q2", "Q3", "Q4", "Jan", "Fév", "Mars", "Avr", "Mai", "Juin", "Juil", "Août", "Sep", "Oct", "Nov", "Déc",
                "Week1", "Week2", "Week3", "Week4", "Week5", "Week6", "Week7", "Week8", "Week9", "Week10", "Week11", "Week12", 
                "Week13", "Week14", "Week15", "Week16", "Week17", "Week18", "Week19", "Week20", "Week21", "Week22", "Week23", 
                "Week24", "Week25", "Week26", "Week27", "Week28", "Week29", "Week30", "Week31", "Week32", "Week33", "Week34", 
                "Week35", "Week36", "Week37", "Week38", "Week39", "Week40", "Week41", "Week42", "Week43", "Week44", "Week45", 
                "Week46", "Week47", "Week48", "Week49", "Week50", "Week51", "Week52", "Week53"}, 
                [Periode]) + 1
        ),
        DataWithProductOrder = Table.AddColumn(DataWithPeriodOrder, "ProduitTri", each if [Produit] = "BASE" then 1 else if [Produit] = "PEAK" then 2 else null),

        // Tri de la table
        DataSorted = Table.Sort(DataWithProductOrder,{{"ProduitTri", Order.Ascending}, {"PeriodeTRI", Order.Ascending}, {"Annee", Order.Ascending}}),

        // Combinaison des colonnes et suppression des colonnes inutiles
        DataCombined = Table.CombineColumns(DataSorted, {"Periode", "Annee", "Produit"}, Combiner.CombineTextByDelimiter(" ", QuoteStyle.None), "Produit_Final"),
        DataFinal = Table.RemoveColumns(DataCombined, {"PeriodeTRI", "ProduitTri"}),

        // Pivot des colonnes pour structurer les données
        DataPivoted = Table.Pivot(DataFinal, List.Distinct(DataFinal[Produit_Final]), "Produit_Final", "Prix_EURMWh", List.Sum),
        DataTypedFinal = Table.TransformColumnTypes(DataPivoted,{{"Date", type date}}),

        // Ajout de la colonne IsWeekend et filtrage des jours de week-end
        DataWithWeekend = Table.AddColumn(DataTypedFinal, "IsWeekend", each Date.DayOfWeek([Date], Day.Monday) >= 5),
        DataWeekday = Table.SelectRows(DataWithWeekend, each [IsWeekend] = false),
        Result = Table.RemoveColumns(DataWeekday,{"SommeDesPrix  ", "IsWeekend"})
    in
        Result
in
    TransformData

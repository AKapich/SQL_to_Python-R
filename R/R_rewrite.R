Sys.setenv(lang="en")
# Dane
install.packages(c("sqldf", "dplyr", "data.table", "compare", "microbenchmark"))


library(sqldf)
library(dplyr)
library(data.table)
library(compare)
library(microbenchmark)


Badges <- read.csv("travel_se_data/Badges.csv.gz")
Comments <- read.csv("travel_se_data/Comments.csv.gz")
PostLinks <- read.csv("travel_se_data/PostLinks.csv.gz")
Posts <- read.csv("travel_se_data/Posts.csv.gz")
Tags <- read.csv("travel_se_data/Tags.csv.gz")
Users <- read.csv("travel_se_data/Users.csv.gz")
Votes <- read.csv("travel_se_data/Votes.csv.gz")



# ZADANIE 1

## sqldf
df_sql_1 <- function(df1){
  result <- sqldf("
    SELECT Count, TagName
    FROM Tags
    WHERE Count > 1000
    ORDER BY Count DESC")
  return(result)
}
result_1_sql <- df_sql_1(Tags)

## base
df_base_1 <- function(){
  #  Wybieramy z ramki danych Tags jedynie kolumny Count i TagName
  df <- subset(Tags, select = c(Count, TagName))
  
  # Wybieramy jedynie wiersze gdzie Count > 1000
  df <- na.omit(df[df$Count>1000,])
  
  # Sortujemy wiersze malejaco po kolumnie Count
  df <- df[order(df$Count, decreasing=TRUE),]
  
  # Numerujemy kolejno wiersze
  rownames(df) <- 1:nrow(df)
  return(df)
}
result_1_base <- df_base_1()

## dplyr
df_dplyr_1 <- function(){

  df <- Tags %>%
    #  Wybieramy z dataframe Tags jedynie kolumny Count i TagName
    select(Count, TagName) %>%
    
    # Wybieramy jedynie wiersze gdzie Count > 1000
    filter(Count>1000) %>%
    
    # Sortujemy wiersze malejaco po kolumnie Count
    arrange(desc(Count))
  
  return(df)
}
result_1_dplyr <- df_dplyr_1()

## data.table
df_table_1 <- function(){
  df <- setDT(Tags)
  
  #  Wybieramy z dataframe Tags jedynie kolumny Count i TagName
  df <- df[,.(Count, TagName)]
  
  # Wybieramy jedynie wiersze gdzie Count > 1000
  df <- df[Count>1000]
  
  # Sortujemy wiersze malejaco po kolumnie Count
  setorder(df, cols=-Count)
  
  return(as.data.frame(df))
}
result_1_table <- df_table_1()

## weryfikacja
all.equal(result_1_base, result_1_sql)
all.equal(result_1_dplyr, result_1_sql)
all.equal(result_1_table, result_1_sql)

microbenchmark::microbenchmark(
  sqldf = df_sql_1(Tags),
  base = df_base_1(),
  dplyr = df_dplyr_1(),
  table = df_table_1()
)



# ZADANIE 2

## sqldf
df_sql_2 <- function(df1, df2){
  result <- sqldf("
    SELECT Location, COUNT(*) AS Count
    FROM (
      SELECT Posts.OwnerUserId, Users.Id, Users.Location
      FROM Users
      JOIN Posts ON Users.Id = Posts.OwnerUserId
    )
    WHERE Location NOT IN ('')
    GROUP BY Location
    ORDER BY Count DESC
    LIMIT 10")
  return(result)
}
result_2_sql <- df_sql_2(Posts, Users)


## base
df_base_2 <- function(){
  # Laczymy dwa dataframes Users i Posts na podstawie identycznych wartosci
  # w kolumnach Users$Id oraz Posts$OwnerUserId
  df <- merge(Users, Posts,
              by.x = "Id", by.y = "OwnerUserId",
              sort=TRUE)
  
  # Ograniczamy nowopowstaly dataframe jedynie do kolumn Id oraz Location
  df <- subset(df, select = c(Id, Location))
  
  # Liczymy ile razy wystepuje kazda z unikalnych 
  # wartosci w wektorze df$Location
  df <- as.data.frame(table(df$Location), stringsAsFactors = FALSE)
  
  # Nadajemy nowe nazwy kolumnom
  colnames(df) <- c("Location", "Count")
  
  # Eliminujemy wiersze gdzie wartosc Location jest pusta
  df <- df[df$Location != "",]

  # Sortujemy dataframe malejaco po kolumnie Count oraz ograniczamy
  # dataframe do 10 wierszy z najwiekszymi wartosciami
  df <- head(df[order(df$Count, decreasing=TRUE),], 10)

  # Numerujemy wiersze
  rownames(df) <- 1:nrow(df)

  return(df)
}
result_2_base <- df_base_2()


## dplyr
df_dplyr_2 <- function(){
  
  df <- Users %>%
    # Laczymy dwa dataframes Users i Posts na podstawie identycznych wartosci
    # w kolumnach Id oraz OwnerUserId
    inner_join(Posts, by=c("Id"="OwnerUserId")) %>%
    
    # Ograniczamy nowopowstaly dataframe jedynie do kolumn Id oraz Location
    select(Id, Location) %>%
    
    # Liczymy ile razy wystepuje kazda z unikalnych 
    # wartosci w wektorze Location
    group_by(Location) %>%
    summarise(Count = n()) %>%
    
    # Eliminujemy wiersze gdzie wartosc Location jest pusta
    filter(Location!="") %>%
    
    # Sortujemy dataframe malejaco po kolumnie Count
    arrange(desc(Count)) %>%
    
    # Ograniczamy dataframe do 10 wierszy z najwiekszymi wartosciami
    head(n=10)
   
  return(as.data.frame(df))
}
result_2_dplyr <- df_dplyr_2()


# table
df_table_2 <- function(){
  df <- setDT(Users)
  # Laczymy dwa dataframes Users i Posts na podstawie identycznych wartosci
  # w kolumnach Id oraz OwnerUserId
  setkey(df, Id)
  setkey(setDT(Posts), OwnerUserId)
  df <- df[setDT(Posts), nomatch=NULL]
  
  # Ograniczamy nowopowstaly dataframe jedynie do kolumn Id oraz Location
  df <- df[,.(Id, Location)]
  
  # Liczymy ile razy wystepuje kazda z unikalnych 
  # wartosci w wektorze Location
  df <- df[, .N, by=Location]
  colnames(df)[2] <- "Count"
  
  # Eliminujemy wiersze gdzie wartosc Location jest pusta
  df <- df[Location != ""]
  # Sortujemy dataframe malejaco po kolumnie Count
  # Ograniczamy dataframe do 10 wierszy z najwiekszymi wartosciami
  df <- head(df[order(-Count)], 10)
    
  return(as.data.frame(df))
}
result_2_table <- df_table_2()

## weryfikacja
all.equal(result_2_base, result_2_sql)
all.equal(result_2_dplyr, result_2_sql)
all.equal(result_2_table, result_2_sql)

microbenchmark::microbenchmark(
  sqldf = df_sql_2(Posts, Users),
  base = df_base_2(),
  dplyr = df_dplyr_2(),
  table = df_table_2()
)


# ZADANIE 3

## sqldf
df_sql_3 <- function(df1){
  result <- sqldf("
    SELECT Year, SUM(Number) AS TotalNumber
    FROM (
      SELECT
        Name,
        COUNT(*) AS Number,
        STRFTIME('%Y', Badges.Date) AS Year
      FROM Badges
      WHERE Class = 1
      GROUP BY Name, Year
    )
    GROUP BY Year
    ORDER BY TotalNumber")
  return(result)
}
result_3_sql <- df_sql_3(Badges)


## base
df_base_3 <- function(){
  
  # Wybieramy wiersze gdzie Class=1
  df <- Badges[Badges$Class==1,]
  
  # Ograniczamy dataframe do kolumn Name oraz Date
  df <- subset(df, select = c(Name, Date))
  
  # Wyciagamy rok z kolumny zawierajacej date
  df$Date <- substr(df$Date, 1, 4)
  colnames(df) <- c("Name", "Year")
  
  # Zliczamy wystapienia niepowtarzalnych 
  # kombinacji wartosci z kolumn df$Name i df$Year
  df <- as.data.frame(table(df$Name, df$Year), stringsAsFactors = FALSE)
  
  # Eliminujemy wiersze z kombinacjami ktore nie wystapily ani razu
  df <- df[df$Freq!=0,]
  
  # Nadajemy nowe nazwy kolumnom i sortujemy dataframe po kolumnach 
  # df$Name i df$Year
  colnames(df) <- c("Name", "Year", "Number")
  df <- df[order(df$Name, df$Year),]
  
  # Sprawdzamy, dla kazdej wartosci z kolumny Year ile wynosi suma wartosci
  # z kolumny Number w wierszach odpowiadajacyh rozpatrywanej
  # wartosci z kolumny Year
  df <- aggregate(Number~Year, df, sum)
  colnames(df) <- c("Year","TotalNumber")
  
  # Sortujemy rosnaco po wartosciach z kolumny TotalNumber
  df <- df[order(df$TotalNumber, decreasing=FALSE),]
  rownames(df) <- 1:nrow(df)
  
  return(df)
}
result_3_base <- df_base_3()

##dplyr
df_dplyr_3 <- function(){
  df <- Badges %>%
    # Wybieramy wiersze gdzie Class=1
    filter(Class==1) %>%
    
    # Ograniczamy dataframe do kolumn Name oraz Date
    select(Name, Date) %>%
    
    # Wyciagamy rok z kolumny zawierajacej date
    mutate(Year = substr(Date, 1, 4)) %>%
    
    # sortujemy dataframe po kolumnach Name i Year
    group_by(Name) %>%
    group_by(Year) %>%
    
    # Sprawdzamy, dla kazdej wartosci z kolumny Year ile wystepuje wierszy
    # z poszczegolnymi wartosciami z kolumny Name
    summarise(TotalNumber=n()) %>%
    
    # Sortujemy rosnaco po wartosciach z kolumny TotalNumber
    arrange(TotalNumber)
  
  return(as.data.frame(df))
}
result_3_dplyr <- df_dplyr_3()

# table
df_table_3 <- function(){
  df <- setDT(Badges)
  # Wybieramy wiersze gdzie Class=1
  df <- df[Class==1]
  
  # Ograniczamy dataframe do kolumn Name oraz Date
  df <- df[,.(Name, Date)]
  
  # Wyciagamy rok z kolumny zawierajacej date
  df <- df[,Date := substr(Date, 1, 4)]
  
  # Sprawdzamy, dla kazdej wartosci z kolumny Date ile wystepuje wierszy
  # z poszczegolnymi wartosciami z kolumny Name
  # Nastepnie kolumne Date przemianowujemy na Year a wynik zliczenia
  # na TotalNumber
  df <- df[,.N, by=(Date)]
  colnames(df) <- c("Year", "TotalNumber")
  
  # Sortujemy rosnaco po wartosciach z kolumny TotalNumber
  df <- df[order(TotalNumber)]
  
  return(as.data.frame(df))
}
result_3_table <- df_table_3()

# weryfikacja
all.equal(result_3_base, result_3_sql)
all.equal(result_3_dplyr, result_3_sql)
all.equal(result_3_table, result_3_sql)

microbenchmark::microbenchmark(
  sqldf = df_sql_3(Badges),
  base = df_base_3(),
  dplyr = df_dplyr_3(),
  table = df_dplyr_3()
)


# ZADANIE 4

## sqldf
df_sql_4 <- function(df1, df2){
  result <- sqldf("
    SELECT
      Users.AccountId,
      Users.DisplayName,
      Users.Location,
      AVG(PostAuth.AnswersCount) as AverageAnswersCount
    FROM
    (
      SELECT
        AnsCount.AnswersCount,
        Posts.Id,
        Posts.OwnerUserId
      FROM (
              SELECT Posts.ParentId, COUNT(*) AS AnswersCount
              FROM Posts
              WHERE Posts.PostTypeId = 2
              GROUP BY Posts.ParentId
            ) AS AnsCount
        JOIN Posts ON Posts.Id = AnsCount.ParentId
    ) AS PostAuth
    JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
    GROUP BY OwnerUserId
    ORDER BY AverageAnswersCount DESC, AccountId ASC
    LIMIT 10")
  return(result)
}
result_4_sql <- df_sql_4(Users, Posts)

## base
df_base_4 <- function(){

  # Wybieramy takie wiersze z dataframe Posts gdzie Posts$PostTypeId=2
  df <- Posts[Posts$PostTypeId==2,]
  
  # Zliczamy ile wystepuje wierszy z kazdym poszczegolnym ParentId
  # Oznaczamy liczbe postow z danym ParentId jako AnswersCount
  # (Liczba odpowiedzi do danego postu-rodzica)
  df <- as.data.frame(table(df$ParentId), stringsAsFactors=FALSE)
  colnames(df) <- c("ParentId", "AnswersCount")
  
  # Zmieniamy typ wartosci w kolumnie df$ParentId tak by zgadzal sie z tym 
  # osiaganym poprzez uzycie sqldf
  df$ParentId <- as.integer(df$ParentId)
                  
  AnsCount <-df
  
  # Laczymy dwa dataframes Posts i AnsCount na podstawie identycznych wartosci
  # w kolumnach Posts$Id oraz AnsCount$ParentId
  df <- merge(Posts, AnsCount,
              by.x = "Id", by.y = "ParentId",
              sort=TRUE)
  
  # Z nowopowstalego dataframe wybieramy jedynie kolumny
  # AnswersCount, Id oraz OwnerUserId
  df <- subset(df, select=c(AnswersCount, Id, OwnerUserId))
  
  PostAuth <- df
  
  # Laczymy dwa dataframes Users i PostAuth na podstawie identycznych wartosci
  # w kolumnach Users$AccountId oraz PostAuth$OwnerUserId
  df <- merge(Users, PostAuth,
              by.x= "AccountId", by.y="OwnerUserId",
              sort=TRUE)
  
  # Z nowopowstalego dataframe wybieramy jedynie kolumny
  # AccountId, DisplayName, Location, AnswersCount
  df <- subset(df, select = c("AccountId", "DisplayName", 
                              "Location", "AnswersCount"))
  
  # Tworzymy pomocniczy dataframe ktory dla kazdej wartosci z kolummny
  # AccountId z ramki danych df liczy srednia arytmetyczna z 
  # wartosci AnswersCount w wierszach odpowiadajacyh 
  # rozpatrywanej wartosci z kolumny AccountId
  auxiliary <- aggregate(AnswersCount~AccountId, df, mean)
  
  # Laczymy dwie ramki danych df i auxiliary na podstawie identycznych 
  # wartosci w kolumnach AccountId
  df <- merge(df, auxiliary,
              by="AccountId")
 
  # Wybieramy z ramki danhych tylko 4 kolumny
  # AccountId, DisplayName, Location. AnswersCount.y 
  df <- subset(df, select=c("AccountId", "DisplayName",
                            "Location", "AnswersCount.y"))
  
  # Wybieramy z ramki danych jedynie unikalne wiersze
  df <- unique(df)
  
  # Zaokraglamy do liczb calkowitych wartosci z kolumny AnswersCount i 
  # nadajemy kolumnie adekwatna nazwe 
  df$AnswersCount.y <- round(df$AnswersCount.y)
  names(df)[4] <- "AverageAnswersCount"
  
  # Sortujemy kolumny w ramce danych po kolumnach
  # AverageAnswersCount oraz AccountId
  df <- df[order(df$AccountId, decreasing = FALSE),]
  df <- df[order(df$AverageAnswersCount, decreasing = TRUE),]
  
  # Ograniczamy dataframe do pierwszych 10 wierszy
  df <- head(df, 10)
  rownames(df) <- 1:nrow(df)
  
  return(as.data.frame(df))
}
result_4_base <- df_base_4()

## dplyr
df_dplyr_4 <- function(){
  AnsCount <- Posts %>%
    # Wybieramy takie wiersze z dataframe Posts gdzie Posts$PostTypeId=2
    filter(PostTypeId==2) %>%
    
    # Zliczamy ile wystepuje wierszy z kazdym poszczegolnym ParentId
    # Oznaczamy liczbe postow z danym ParentId jako AnswersCount
    # (Liczba odpowiedzi do danego postu-rodzica)
    group_by(ParentId) %>%
    summarise(AnswersCount=n()) %>%
    
    # Zmieniamy typ wartosci w kolumnie df$ParentId tak by zgadzal sie z tym 
    # osiaganym poprzez uzycie sqldf
    mutate(ParentId = as.integer(ParentId)) %>%
    as.data.frame()
  
  PostAuth <- Posts %>%
    # Laczymy dwa dataframes Posts i AnsCount na podstawie identycznych wartosci
    # w kolumnach Posts$Id oraz AnsCount$ParentId
    inner_join(AnsCount, by=c("Id"="ParentId")) %>%
    
    # Z nowpowstalego dataframe wybieramy jedynie kolumny
    # AnswersCount, Id oraz OwnerUserId
    select(AnswersCount, Id, OwnerUserId) %>%
    as.data.frame()
  
  df <- Users %>%
    # Laczymy dwa dataframes Users i PostAuth na podstawie identycznych wartosci
    # w kolumnach Users$AccountId oraz PostAuth$OwnerUserId
    inner_join(PostAuth, by=c("AccountId"="OwnerUserId")) %>%
    
    # Z nowopowstalego dataframe wybieramy jedynie kolumny
    # AccountId, DisplayName, Location, AnswersCount
    select(AccountId, DisplayName, Location, AnswersCount)
    
  auxiliary <- df %>%
    # Tworzymy pomocniczy dataframe ktory dla kazdej wartosci AccountId
    # z dataframe df liczy srednia arytmetyczna z wartosci AnswersCount w 
    # wierszach odpowiadajacyh danej wartosci z kolumny AccountId
    group_by(AccountId) %>%
    summarise(AverageAnswersCount=round((mean(AnswersCount))))
  
  df <- df %>%
    # Laczymy dwie ramki danych df i auxiliary na podstawie identycznych 
    # wartosci w kolumnach AccountId
    inner_join(auxiliary, by="AccountId") %>%
    
    # Wyrzucamy nie interesujaca nas kolumne AnswersCount
    select(-AnswersCount) %>%
    # Wybieramy z ramki danych jedynie unikalne wiersze
    distinct() %>%
    
    # Sortujemy kolumny w ramce danych po kolumnach
    # AverageAnswersCount oraz AccountId
    arrange(AccountId) %>%
    arrange(desc(AverageAnswersCount)) %>%
    
    # Ograniczamy dataframe do pierwszych 10 wierszy i
    head(n=10)

  return(as.data.frame(df))
}
result_4_dplyr <- df_dplyr_4()

## table
df_table_4 <- function(){
  AnsCount <- setDT(Posts)
  # Wybieramy takie wiersze z dataframe Posts gdzie Posts$PostTypeId=2
  AnsCount <- AnsCount[PostTypeId==2]
  
  # Zliczamy ile wystepuje wierszy z kazdym poszczegolnym ParentId
  # (Liczba odpowiedzi do danego postu-rodzica)
  AnsCount <- AnsCount[,.N, by=(ParentId)]
  AnsCount <- AnsCount[order(ParentId)]
  # Oznaczamy liczbe postow z danym ParentId jako AnswersCount
  colnames(AnsCount)[2] <- "AnswersCount"
  
  
  PostAuth <- setDT(Posts)
  # Laczymy dwa dataframes Posts i AnsCount na podstawie identycznych wartosci
  # w kolumnach Id oraz ParentId
  PostAuth <- PostAuth[AnsCount, on=c("Id"="ParentId")]
  PostAuth <- PostAuth[,.(AnswersCount, Id, OwnerUserId)]
  
  df <- setDT(Users)
  # Laczymy dwa dataframes Users i PostAuth na podstawie identycznych wartosci
  # w kolumnach AccountId oraz OwnerUserId
  setkey(df, AccountId)
  setkey(PostAuth, OwnerUserId)
  df <- df[PostAuth, nomatch=NULL]
  # Z nowopowstalego dataframe wybieramy jedynie kolumny
  # AnswersCount, Id oraz OwnerUserId
  df <- df[,.(AccountId, DisplayName, Location, AnswersCount)]
  
  # Tworzymy pomocniczy dataframe ktory dla kazdej wartosci z kolummny
  # AccountId z ramki danych df liczy srednia arytmetyczna z 
  # wartosci AnswersCount w wierszach odpowiadajacyh 
  # rozpatrywanej wartosci z kolumny AccountId
  auxiliary <- df[,.(round((mean(AnswersCount)))),by=(AccountId)]
  
  # Laczymy dwie ramki danych df i auxiliary na podstawie identycznych 
  # wartosci w kolumnach AccountId
  df <- df[auxiliary, on=.(AccountId)]
  df <- df[,AnswersCount :=NULL]
  colnames(df)[4] <- "AverageAnswersCount"
  
  # Wybieramy z ramki danych jedynie unikalne wiersze
  df <- unique(df)
  
  # Sortujemy kolumny w ramce danych po kolumnach
  # AverageAnswersCount malejaca oraz AccountId rosnaco
  # Ograniczamy wynik do pierwszych 10 wierszy
  df <- head(df[order(-AverageAnswersCount, AccountId)], 10)
  
  return(as.data.frame(df))
}
result_4_table <- df_table_4()

# weryfikacja
all.equal(result_4_base, result_4_sql)
all.equal(result_4_dplyr, result_4_sql)
all.equal(result_4_table, result_4_sql)

microbenchmark::microbenchmark(
  sqldf = df_sql_4(Posts, Users),
  base = df_base_4(),
  dplyr = df_dplyr_4(),
  table = df_table_4(),
  times=10
)



# Zadanie 5

## sqldf
df_sql_5 <- function(df1, df2){
  result <- sqldf("
    SELECT Posts.Title, Posts.Id,
            STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
            VotesByAge.Votes
    FROM Posts
    JOIN (
            SELECT
                PostId,
                MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
                MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
                SUM(Total) AS Votes
            FROM (
                SELECT
                    PostId,
                    CASE STRFTIME('%Y', CreationDate)
                        WHEN '2021' THEN 'new'
                        WHEN '2020' THEN 'new'
                        ELSE 'old'
                        END VoteDate,
                    COUNT(*) AS Total
                FROM Votes
                WHERE VoteTypeId IN (1, 2, 5)
                GROUP BY PostId, VoteDate
            ) AS VotesDates
            GROUP BY VotesDates.PostId
            HAVING NewVotes > OldVotes
    ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
    WHERE Title NOT IN ('')
    ORDER BY Votes DESC
    LIMIT 10")
  return(result)
}
result_5_sql <- df_sql_5(Posts, Votes)


## base
df_base_5 <- function(){
  # Eliminujemy z dataframe Votes wiersze gdzie wartosc VoteTypeId
  # nie nalezy do zbioru {1,2,5}
  df <- Votes[Votes$VoteTypeId %in% c(1,2,5),]

  # Tworzymy pomocniczy wektor zawierajacy informacje na temat tego czy data
  # z kolumny CreationDate pochodzi z roku co najmniej 2020
  # Wartosc 1 oznacza ze tak, wartosc 0 oznacza ze nie
  auxiliary <- df$CreationDate
  auxiliary <- substring(auxiliary, 1, 4)
  auxiliary <- as.data.frame(as.integer(c(as.numeric(auxiliary)>2019)))

  # Dolaczamy wektor pomocniczy do glownej ramki danych
  df <- cbind(df, auxiliary)
  names(df)[7] = "VoteDate"

  # Zliczamy ile razy wystepuja poszczegolne kombinacje wartosci z kolumn
  # PostId i VoteDate
  # Innymi slowy zliczamy dla kazdego PostId zliczamy ile powstalo starych
  # oraz ile powstalo nowych postow
  df <- as.data.frame(table(df$PostId, df$VoteDate), stringsAsFactors=FALSE)
  colnames(df) <- c("PostId", "VoteDate", "Total")

  # Sortujemy dataframe po wartosciach z kolumn VoteDate i PostId
  df <- df[order(df$VoteDate, decreasing=TRUE),]
  df <- df[order(df$PostId),]
  rownames(df) <- 1:nrow(df)

  VotesDates <- df

  # Sprawdzamy dla kazdej wartosci PostId kolejno ile istnieje "starych" postow,
  # "nowych" postow oraz ile istnieje lacznie
  total <- aggregate(Total~PostId, VotesDates, sum)
  new <- VotesDates[VotesDates$VoteDate==1,]
  names(new)[3] <- "NewVotes"
  old <- VotesDates[VotesDates$VoteDate==0,]
  names(old)[3] <- "OldVotes"

  # stworzone wczesniej wektory laczymy w ramke danych na podstawie wartosci
  # z kolumny PostId
  df <- merge(new, old,
        by="PostId")
  df <- merge(df, total,
              by="PostId")
  names(df)[6] = "Votes"

  # Ograniczamy ramke danych do 4 interesujacych nas kolumn
  df <- subset(df, select=c("PostId", "NewVotes", "OldVotes", "Votes"))

  # Pozostawiamy w ramce danych jedynie te wiersze, gdzie NewVotes>OldVotes
  df <- df[df$NewVotes > df$OldVotes,]

  # Sortujemy ramke danych wzgledem kolumny PostId aby dostac taki sam wynik
  # jak sqldf
  df <- df[order(df$PostId),]
  rownames(df) <- 1:nrow(df)

  VotesByAge <- df

  # Laczymy ramki danych VotesByAge i Posts na podstawie wartosci w kolumnach
  # VotesByAge$PostId oraz Posts$Id
  df <- merge(VotesByAge, Posts,
              by.x="PostId", by.y="Id")

  # Tworzymy mniejsza ramke danych skladajaca sie z kolumn Id oraz CreationDate
  # z ramki danych Posts
  Date <- subset(Posts, select=c("Id", "CreationDate"))

  # Z kolumny CreationDate wyciagamy date dzienna
  Date$CreationDate <- substring(Date$CreationDate, 1, 10)
  names(Date)[2] <- "Date"

  # Laczymy uprzednio stworzona ramke danych z mniejsza ramka danych
  # zawierajaca informacje na temat daty. Ramki danych laczymy na podstawie
  # kolumn zawierajacych Id postow
  df <- merge(df, Date,
              by.x="PostId", by.y="Id")

  # Ograniczamy ramke danych do interesujacych nas kolumn
  df <- subset(df, select=c("Title", "PostId","Date", "Votes"))
  names(df)[2] <- "Id"

  # Zamieniamy kolumne wartosci "character" na kolumne wartosci liczbowych
  df$Id <- as.integer(df$Id)

  # Usuwamy wiersze gdzie zawartosc kolumny Title jest pusta
  df <- df[df$Title!="",]

  # Sortujemy wiersze po kolumnie Votes oraz ograniczamy ramke danych do
  # pierwszych 10 wierszy
  df <- head(df[order(df$Votes, decreasing = TRUE),],10)
  rownames(df) <- 1:nrow(df)

  return(df)
}
result_5_base <- df_base_5()

## dplyr
df_dplyr_5 <- function(){
  VotesDates <- Votes %>%
    # Eliminujemy z dataframe Votes wiersze gdzie wartosc VoteTypeId
    # nie nalezy do zbioru {1,2,5}
    filter(VoteTypeId %in% c(1,2,5)) %>%
    # Tworzymy pomocnicza kolumne zawierajaca informacje czy data
    # z kolumny CreationDate pochodzi z roku co najmniej 2020
    # Wartosc 1 oznacza ze tak, wartosc 0 oznacza ze nie
    mutate(VoteDate=as.numeric(as.integer(substring(CreationDate, 1, 4))>2019))
    
    # Sprawdzamy dla kazdej wartosci PostId ile istnieje lacznie postow
    Total <- VotesDates %>%
      group_by(VoteDate) %>%
      group_by(PostId) %>%
      summarise(Votes = n())
    
    # Sprawdzamy dla kazdej wartosci PostId ile istnieje nowych postow
    New <- VotesDates %>%
      filter(VoteDate==1) %>%
      group_by(PostId) %>%
      summarise(NewVotes = n())
    
    # Sprawdzamy dla kazdej wartosci PostId ile istnieje starych postow
    Old <- VotesDates %>%
      filter(VoteDate==0) %>%
      group_by(PostId) %>%
      summarise(OldVotes = n())
    
    VotesByAge <- Total %>%
      # stworzone wczesniej wektory laczymy w ramke danych na podstawie wartosci
      # z kolumny PostId
      left_join(New, by="PostId") %>%
      left_join(Old, by="PostId") %>%
      replace(is.na(.),0) %>%
      # Pozostawiamy w ramce danych jedynie te wiersze, gdzie NewVotes>OldVotes
      filter(NewVotes>OldVotes) %>%
      mutate(PostId = as.numeric(PostId))
    
    Date <- Posts %>%
      # Tworzymy mniejsza ramke danych skladajaca sie z kolumn Id oraz CreationDate
      # z ramki danych Posts
      select(Id, CreationDate) %>%
      # Z kolumny CreationDate wyciagamy date dzienna
      mutate(Date = substring(CreationDate, 1, 10)) %>%
      select(-CreationDate)
      
    df <- VotesByAge %>%
      # Laczymy uprzednio stworzona ramke danych VotesByAge
      # z ramka danych Posts i ramka danych Date
      # Ramki danych laczymy na podstawie kolumn zawierajacych Id postow
      inner_join(Posts, by=c("PostId"="Id")) %>%
      inner_join(Date, by=c("PostId"="Id")) %>%
      
      # Ograniczamy ramke danych do interesujacych nas kolumn
      select(Title, PostId, Date, Votes) %>%
      mutate(Id := as.integer(PostId)) %>%
      select(-PostId) %>%
      
      # Usuwamy wiersze gdzie zawartosc kolumny Title jest pusta
      filter(Title != "") %>%
      
      # Sortujemy wiersze po kolumnie Votes oraz ograniczamy ramke danych do
      # pierwszych 10 wierszy
      arrange(desc(Votes)) %>%
      head(n=10) %>%
      relocate(Id, .before=Date)

  return(as.data.frame(df))
}
result_5_dplyr <- df_dplyr_5()

## table
df_table_5 <- function(){
  df <- setDT(Votes)
  # Eliminujemy z ramki danych Votes wiersze gdzie wartosc VoteTypeId
  # nie nalezy do zbioru {1,2,5}
  df <- df[VoteTypeId %in% c(1,2,5)]
  
  
  # Tworzymy pomocnicza kolumne zawierajacy informacje na temat tego czy data
  # z kolumny CreationDate pochodzi z roku co najmniej 2020
  # Wartosc 1 oznacza ze tak, wartosc 0 oznacza ze nie
  df <- df[,VoteDate := substring(CreationDate,1,4)]
  df <- df[, VoteDate := as.integer(as.numeric(VoteDate)>2019)]
  
  # Pogrupowane i zsumowane po VoteDate i PostId
  # Sprawdzamy dla kazdego PostId ile istnieje nowych postow, starych
  # postow oraz postow lacznie
  Total <- df[, .N, by=PostId]
  colnames(Total)[2] = "Votes"
  New <- df[VoteDate==1][, .N, by=PostId]
  colnames(New)[2] = "NewVotes"
  Old <- df[VoteDate==0][, .N, by=PostId]
  colnames(Old)[2] = "OldVotes"
  
  # Tworzymy nowa ramke danych z ramek Total, New, Old\
  # na podstawie wartosci z kolumny PostId
  VotesByAge <- merge(Total, New, all.x=TRUE)
  VotesByAge <- merge(VotesByAge, Old, all.x=TRUE)
  VotesByAge[is.na(VotesByAge)] <- 0
  # Pozostawiamy w ramce danych jedynie te wiersze, gdzie NewVotes>OldVotes
  VotesByAge <- VotesByAge[NewVotes>OldVotes]
  
  # Tworzymy mniejsza ramke danych skladajaca sie z kolumn Id oraz CreationDate
  # z ramki danych Posts
  Date <- setDT(Posts)[,.(Id, CreationDate)]
  # Z kolumny CreationDate wyciagamy date dzienna
  Date <- Date[,Date := substring(CreationDate, 1, 10)][,CreationDate := NULL]
  
  # Laczymy uprzednio stworzona ramke danych VotesByAge
  # z ramka danych Posts i ramka danych Date
  # Ramki danych laczymy na podstawie kolumn zawierajacych Id postow
  df <- VotesByAge[Posts, on=c("PostId"="Id")]
  df <- df[Date, on=c("PostId"="Id")]
  # Ograniczamy ramke danych do interesujacych nas kolumn
  df <- df[,.(Title, PostId, Date, Votes)]
  # Usuwamy wiersze gdzie zawartosc kolumny Title jest pusta
  # Sortujemy wiersze po kolumnie Votes malejaco
  df <- df[,Id := PostId][,PostId := NULL][Title != ""][order(-Votes)]
  # Ograniczamy ramke danych do pierwszych 10 wierszy oraz
  # przestawiamy szyk kolumn
  df <- head(df, 10)
  setcolorder(df, neworder=c("Title", "Id", "Date", "Votes"))
  
  return(as.data.frame(df))
}
result_5_table <- df_table_5()

# weryfikacja
all.equal(result_5_base, result_5_sql)
all.equal(result_5_dplyr, result_5_sql)
all.equal(result_5_table, result_5_sql)

microbenchmark::microbenchmark(
  sqldf = df_sql_5(Posts, Votes),
  base = df_base_5(),
  dplyr = df_dplyr_5(),
  table = df_table_5(),
  times=10
)

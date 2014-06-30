tdConnString <- "DRIVER=Teradata;DBCNAME=54.86.41.115;DATABASE=RevoTestDB;UID=revo;PWD=revo;"
tdQuery <- "SELECT * FROM RevoTestDB.ccFraud10"
teradataDS <- RxTeradata(connectionString=tdConnString, sqlQuery=tdQuery, rowsPerHead=50000)
rxGetVarInfo(data=teradataDS)
stateAbb <- c("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC",
"DE", "FL", "GA", "HI","IA", "ID", "IL", "IN", "KS", "KY", "LA",
"MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NB", "NC", "ND",
"NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI","SC",
"SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY")
ccColInfo <- list(
	gender = list(
	type = "factor",
	levels = c("1", "2"),
	newLevels = c("Male", "Female")),
	cardholder = list(
	type = "factor",
	levels = c("1", "2"),
	newLevels = c("Principal", "Secondary")),
	state = list(
	type = "factor",
	levels = as.character(1:51),
	newLevels = stateAbb)
)
teradataDS <- RxTeradata(connectionString=tdConnString,
sqlQuery=tdQuery, colInfo=ccColInfo, rowsPerHead=50000)
rxGetVarInfo(data=teradataDS)

tdShareDir <- paste("D:\\Users\\revo\\Documents\\AllShare\\", Sys.getenv("USERNAME"), sep="")
tdRemoteShareDir <- "/tmp/revoJobs"
tdRevoPath <- "/usr/lib64/Revo-7.1/R-3.0.2/lib64/R"
dir.create(tdShareDir, recursive = TRUE)
tdWait <- TRUE
tdConsoleOutput <- FALSE
tdCompute <- RxInTeradata(
	connectionString=tdConnString,
	shareDir=tdShareDir,
	remoteShareDir=tdRemoteShareDir,
	revoPath=tdRevoPath,
	wait=tdWait,
	consoleOutput=tdConsoleOutput)

rxGetNodeInfo(tdCompute)

# Set the compute context to compute in Teradata
rxSetComputeContext(tdCompute)

rxSummary(formula = ~gender + balance + numTrans + numIntlTrans + creditLine, data = teradataDS)

# Set the compute context to compute locally
rxSetComputeContext("local")

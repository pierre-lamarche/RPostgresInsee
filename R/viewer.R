#' @importFrom utils vignette
NULL

# Le code du viewer est largement inspiré du code source du package odbc
# https://github.com/r-dbi/odbc/blob/master/R/Viewer.R
# Licence : MIT - Copyright : RStudio

on_connection_updated <- function(connection, hint) {

  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))


  observer$connectionUpdated(
    type = "Postgre",
    host = DBI::dbGetInfo(connection)$dbname,
    hint = hint)
}

on_connection_opened <- function(connection, code) {


  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))

  # let observer know that connection has opened
  observer$connectionOpened(

    # connection type
    type = "Postgre",

    # name displayed in connection pane
    displayName = paste("PostgreSQL SDSE // db :", DBI::dbGetInfo(connection)$dbname),

    # host key
    host = DBI::dbGetInfo(connection)$dbname,

    # icon for connection
    icon = (system.file("img", "ssp16x16.png", package = "pgsdse")),

    # connection code
    connectCode = code,

    # disconnection code
    disconnect =  function(...){
      DBI::dbDisconnect(new("PgInseeConnection", connection))
    },

    # object types structure code
    listObjectTypes = function () {
      pgInseeListObjectTypes()
    },

    # table enumeration code
    listObjects = function(...) {
      res <- pgInseeListObjects(connection, ...)
    },

    # column enumeration code
    listColumns = function(...) {
      pgInseeListColumns(connection, ...)
    },

    # table preview code
    previewObject = function(rowLimit, ...) {
      pgInseePreviewObject(connection, rowLimit, ...)
    },

    # other actions that can be executed on this connection
    actions = pgInseeConnectionActions(connection),

    # raw connection object
    connectionObject = connection
  )
}



on_connection_closed <- function(connection) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))

  type <- "Postgre"
  host <- DBI::dbGetInfo(connection)$dbname
  observer$connectionClosed(type, host)
}


# un cluster PostgreSQL est composé de "catalog"
# les "catalog" contiennent des "schemas"
# les "schemas" contiennent des "table" et des "view"
# les "table" et les "view" contiennent les données
pgInseeListObjectTypes <- function() {

  obj_types <- list(table = list(contains = "data"))
  obj_types <- c(obj_types, list(view = list(contains = "data")))
  obj_types <- list(schema = list(contains = obj_types))
  obj_types <- list(catalog = list(contains = obj_types))

  return(obj_types)
}

# Liste les objets présents dans la base PostgreSQL
# répond aux spécifications du connection contract
# https://rstudio.github.io/rstudio-extensions/connections-contract.html
pgInseeListObjects <- function(connection,
                              catalog = NULL,
                              schema = NULL,
                              table = NULL,
                              view = NULL,
                              name = NULL,
                              type = NULL,
                              ...) {

  if(is.null(catalog) && is.null(schema)) {
    return(pgInseeListCatalogs(connection, ...))
  }

  if(is.null(schema)) {
    return(pgInseeListSchemas(connection, catalog, ...))
  }

  return(pgInseeListTablesAndViews(connection, catalog, schema, ...))

}

# cette fonction retourne le catalogue actif
pgInseeListCatalogs <- function(connection, ...) {
  query <- "select current_database();"
  obj <- DBI::dbGetQuery(connection, query)
  obj$type <- "catalog"
  colnames(obj) <- c("name","type")
  return(obj)
}


# retourne la liste des schemas pour le catalogue courant
pgInseeListSchemas <- function(connection, catalog, ...) {
  query <- "select nspname as name from pg_catalog.pg_namespace
            where nspacl is not null
            and nspname not in ('public', 'pg_catalog', 'information_schema')
            order by name"
  obj <- DBI::dbGetQuery(connection, query)
  obj$type <- "schema"
  return(obj)
}

# retourne la liste des tables et des vues pour le catalogue courant
# et le schema passé en paramètre
pgInseeListTablesAndViews <- function(connection, catalog, schema, ...) {
  query <- sprintf("select table_type as type, table_name as name
            from information_schema.tables
            where table_schema not in ('pg_catalog', 'information_schema')
            and table_schema in ('%s')
            and table_schema not like 'pg_toast%%'
            order by name", schema)
  obj <- DBI::dbGetQuery(connection, query)
  if(nrow(obj)>0) {
    obj$type <- tolower(obj$type)
    obj[which(obj$type=="base table"),"type"] <- "table"
  }
  return(obj)
}

# retourne la liste des colonnes d'une table ou d'une vue
pgInseeListColumns <- function(connection, catalog, schema, table = NULL, view = NULL) {
  name  <- unique(c(table, view))
  query <- sprintf("select * from %s.%s where 0=1",schema, name)
  res   <- DBI::dbSendQuery(connection, query)
  on.exit(DBI::dbClearResult(res))
  infos <- DBI::dbColumnInfo(res)
  return(infos[, c("name", "type")])
}

# retourne les premières entrée de la table ou de la vue passée
# en paramètre
pgInseePreviewObject <- function(connection, rowLimit, catalog=NULL,
                                schema = NULL, table = NULL, view = NULL) {
  name <- unique(c(table, view))
  query <- sprintf("select * from %s.%s limit %i", schema, name, rowLimit)
  DBI::dbGetQuery(connection, query)
}

pgInseeConnectionActions <- function(connection) {
  list(
    Help = list(
      icon = "",
      callback = function() {
      }
    )
  )
}

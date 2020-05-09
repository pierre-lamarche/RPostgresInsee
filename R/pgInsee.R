#' @import DBI
#' @import RPostgres
#' @import methods
#' @include viewer.R
NULL

#' @importClassesFrom RPostgres PqConnection
setClass("PgInseeConnection", contains = c("PqConnection", "DBIConnection"))

#' @importClassesFrom RPostgres PqDriver
setClass("PgInseeDriver", contains = c("PqDriver", "DBIDriver"))

#' Connexion au serveur Postgres
#'
#' Se connecter au serveur Postgres, en visualisant la structure de la base de données dans l'onglet Connections de RStudio
#'
#' Cette méthode surcharge le comportement de la méthode parente de la classe \code{PqConnection}
#'
#' @param drv Un objet de type driver \code{RPostgres PqDriver}
#' @param ... d'autres paramètres concernant la base de données sur laquelle se connecter. Consulter la documentation sur [DBI::dbConnect()]
#' @return Un objet de la classe `PgInseeConnection` permettant de communiquer avec les bases de données Postgres à l'Insee.
#' @inherit RPostgres::dbConnect
#' @export
setMethod("dbConnect", "PgInseeDriver", function(drv, ...) {
  # on s'arrête immédiatement si le driver n'est pas du type attendu
  stopifnot(inherits(drv, "PqDriver"))

  callNextMethod(drv, ...)

  # Notification au panneau de connexion de RStudio
  # Code copié depuis https://github.com/r-dbi/odbc/blob/74ad62c275b1c95a70814707901bdafd8fc2d2b6/R/Driver.R#L112
  # Licence : MIT - Copyright : RStudio
  # perform the connection notification at the top level, to ensure that it's had
  # a chance to get its external pointer connected, and so we can capture the
  # expression that created it
  if (!is.null(getOption("connectionObserver"))) {
    addTaskCallback(function(expr, ...) {
      tryCatch({
        if (is.call(expr) &&
            as.character(expr[[1]]) %in% c("<-", "=") &&
            "connectPostgreSDSE" %in% as.character(expr[[3]][[1]])) {
          # notify if this is an assignment we can replay
          on_connection_opened(
            connection = eval(expr[[2]]),
            code = paste(c(deparse(expr)), collapse = "\n")
          )
        }
      }, error = function(e) {
        warning("Could not notify connection observer. ", e$message, call. = FALSE)
      })
      # always return false so the task callback is run at most once
      FALSE
    })
  }
})

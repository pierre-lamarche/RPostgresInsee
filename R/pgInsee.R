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
#' @param host L'URL du serveur Postgres
#' @param port Le port pour se connecter au serveur Postgres
#' @param dbname Le nom de la base de données
#' @param user Par défaut, l'Idep de l'utilisateur
#' @param ... D'autres paramètres concernant la base de données sur laquelle se connecter. Consulter la documentation sur [DBI::dbConnect()]
#' @return Un objet de la classe `PgInseeConnection` permettant de communiquer avec les bases de données Postgres à l'Insee.
#' @inherit RPostgres::dbConnect
#' @export
setMethod("dbConnect", "PgInseeDriver", function(drv, host, port, dbname, user, ...) {
  # on s'arrête immédiatement si le driver n'est pas du type attendu
  stopifnot(inherits(drv, "PqDriver"))

  connexion <- callNextMethod(drv, host = host, port = port, dbname = dbname,
                              user = system("whoami", intern = TRUE),
                              password = rstudioapi::askForPassword("Entrez votre mot de passe Windows :"),
                              ...)

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
  # affichage du message d'acceuil
  cat(sprintf("
              -----------------------------------------
              | connexion en PostgreSQL - Insee       |
              -----------------------------------------
              | %s
              -----------------------------------------
              | PKG VERSION\tv.%s
              | HOST\t%s
              | PORT\t%s
              | DBNAME\t%s
              -----------------------------------------\n\n",
              user, utils::packageDescription("RPostgresInsee")$Version,
              host, port, dbname))

  # on veille bien à retourner un objet de classe PgInseeConnection
  # qui surcharge le comportement par défaut de la classe RPostgres PqConnection
  # notamment à la déconnexion en notifiant le connection pane de RStudio
  return(new("PgInseeConnection", connexion))
})


#' Déconnexion du serveur Postgres
#'
#' Permet de se déconnecter du serveur - recommandé
#'
#' Cette méthode surcharge le comportement par défaut de la classe parente \code{PqConnection}.
#'
#' @param conn Un objet de type PgInseeConnection
#' @param ... D'autres paramètres à transmettre. Consulter la documentation sur [DBI::dbDisconnect()]
#' @return None - affiche un simple message de déconnexion.
#' @inherit RPostgres::dbDisconnect
#' @export
setMethod("dbDisconnect", "PgInseeConnection", function(conn, ...) {
  # notifier le panneau de connection de RStudio
  on_connection_closed(conn)

  # appeller la méthode dbDisconnect telle que définie
  # dans la classe parente "PqConnection" de RPostgres
  # de façon à clore la connection, annuler les tâches en cours
  # et libérer les ressources (e.g., memory, sockets).
  # + affichage d'un message de confirmation à l'utilisateur

  cat(ifelse(callNextMethod(...),
             "D\u00e9connexion de la base PostgreSQL confirm\u00e9e\n",
             "D\u00e9connexion de la base PostgreSQL en \u00e9chec\n"))
})

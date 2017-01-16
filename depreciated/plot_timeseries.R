#' quick interactive plot for timeseries
#' @param plot.data a tbl object
#' @param column the column containing values to be calculated
#' @param xaxis the x-axis variable name, usually supplied by date or time type
#' @import dplyr
#' @import rCharts
#' @export

plot_timeseries <- function(plot.data,xaxis,...){

  parameters <- as.list(match.call())

  xaxis <- as.character(parameters[['xaxis']])
  yaxis <- sapply(4:length(parameters), function(x){as.character(parameters[[x]])})

  plot.data <- plot.data %>% as.data.frame
  plot.data[[xaxis]] <- as.character(plot.data[[xaxis]])

  ##
  m1 <- mPlot(x = xaxis, y = yaxis,
              type = "Line", data = plot.data)
  m1$set(pointSize = 2, lineWidth = 3)
  return(m1)

}#end function


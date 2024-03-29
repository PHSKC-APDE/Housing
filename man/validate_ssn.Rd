% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validate_ssn.R
\name{validate_ssn}
\alias{validate_ssn}
\title{Cleans SSN}
\usage{
validate_ssn(DTx, ssnx = NULL)
}
\arguments{
\item{DTx}{A data.table}

\item{ssnx}{Name of variable to check for accuracy of a SSN or PHA ID}
}
\description{
\code{validate_ssn} tidies and standardizes a designated SSN column when 
possible and sets the value equal to NA when the value is clearly not an SSN. 
Distinct from the \code{junk_ssn} function in this package.
}

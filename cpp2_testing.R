library(NHSRwaitinglist)
library(Rcpp)
sourceCpp("wl_simulator.cpp")


# Test individual functions
sim_schedule_cpp(10, Sys.Date(), 1.5)

sim_schedule(10,Sys.Date(), 1.5)

wl_join_cpp(data.frame(Referral = Sys.Date()), data.frame(Referral = Sys.Date() + 1))

wl_join(data.frame(Referral = Sys.Date()), data.frame(Referral = Sys.Date() + 1))



wl_schedule_cpp(data.frame(Referral = Sys.Date() + 0:4, Removal = as.Date(rep(NA, 5))),
                Sys.Date() + 1:10)

wl_schedule(data.frame(Referral = Sys.Date() + 0:4, Removal = as.Date(rep(NA, 5))),
                Sys.Date() + 1:10)


# CPPFull simulator
set.seed(123)
a<-wl_simulator_cpp(start_date = Sys.Date(), end_date = Sys.Date() + 20, demand = 10, capacity = 11)

set.seed(123)
# R simulator
b<-wl_simulator(start_date = Sys.Date(), end_date = Sys.Date() + 20, demand = 10, capacity = 11)

identical(a,b)



set.seed(123)
r_out <- wl_simulator(Sys.Date(), Sys.Date() + 30)
set.seed(123)
cpp_out <- wl_simulator_cpp2(Sys.Date(), Sys.Date() + 30)


Rcpp::sample(5, 105, true)
sample(..., replace = TRUE)




library(Rcpp)
library(data.table)
sourceCpp("wl_schedule_cpp.cpp")

waiting_list <- data.frame(Referral = Sys.Date() + 0:4,
                           Removal = as.Date(rep(NA, 5)))

schedule <- Sys.Date() + 1:10

# Test unscheduled = FALSE
wl_schedule_cpp(waiting_list, schedule)
# Expected:
#   Referral    Removal
# 1 2025-12-01 2025-12-02
# 2 2025-12-02 2025-12-03
# 3 2025-12-03 2025-12-04
# 4 2025-12-04 2025-12-05
# 5 2025-12-05 2025-12-06

# Test unscheduled = TRUE
wl_schedule_cpp(waiting_list, schedule, unscheduled = TRUE)



# Set seed for reproducibility
set.seed(123)

# Call your C++ function
result_cpp <- wl_simulator_cpp(
    start_date = as.Date("2025-01-01"),
    end_date = as.Date("2025-02-01"),
    demand = 10,
    capacity = 11
)

# Reset seed and call R function for comparison
set.seed(123)

result_r <- wl_simulator(
    start_date = as.Date("2025-01-01"),
    end_date = as.Date("2025-02-01"),
    demand = 10,
    capacity = 11
)

# Compare results
all.equal(result_cpp, result_r)


**Key points:**
    - `set.seed(123)` sets the R global seed, which C++ functions access via `R::rpois()` and `R::runif()`
- Call `set.seed()` **before** each function call to get identical sequences
- Both functions will generate identical random numbers when using the same seed

**Verify they match:**
    ````r
# Check if outputs are identical
identical(result_cpp, result_r)

# Or detailed comparison
str(result_cpp)
str(result_r)
head(result_cpp)
head(result_r)
#include <Rcpp.h>
using namespace Rcpp;

// Geometric random numbers
IntegerVector rgeom_cpp(int n, double prob) {
    IntegerVector out(n);
    for(int i = 0; i < n; i++) {
        out[i] = R::rgeom(prob);
    }
    return out;
}

// Corrected wl_join_cpp: replicates R's rbind + sort
// [[Rcpp::export]]
DataFrame wl_join_cpp(DataFrame wl_1, DataFrame wl_2, int referral_index = 0) {
    int n1 = wl_1.nrows();
    int n2 = wl_2.nrows();
    int n_total = n1 + n2;

    CharacterVector col_names = wl_1.names();
    int n_cols = col_names.size();
    List combined(n_cols);
    combined.attr("names") = col_names;

    // Combine columns
    for(int c = 0; c < n_cols; c++) {
        SEXP col1 = wl_1[c];
        SEXP col2 = wl_2[c];

        switch(TYPEOF(col1)) {
        case REALSXP: {
            NumericVector v_total(n_total);
            NumericVector v1(col1), v2(col2);
            for(int i = 0; i < n1; i++) v_total[i] = v1[i];
            for(int i = 0; i < n2; i++) v_total[n1 + i] = v2[i];
            combined[c] = v_total;
            break;
        }
        case INTSXP: {
            IntegerVector v_total(n_total);
            IntegerVector v1(col1), v2(col2);
            for(int i = 0; i < n1; i++) v_total[i] = v1[i];
            for(int i = 0; i < n2; i++) v_total[n1 + i] = v2[i];
            combined[c] = v_total;
            break;
        }
        case STRSXP: {
            CharacterVector v_total(n_total);
            CharacterVector v1(col1), v2(col2);
            for(int i = 0; i < n1; i++) v_total[i] = v1[i];
            for(int i = 0; i < n2; i++) v_total[n1 + i] = v2[i];
            combined[c] = v_total;
            break;
        }
        case VECSXP: {
            List v_total(n_total);
            List v1(col1), v2(col2);
            for(int i = 0; i < n1; i++) v_total[i] = v1[i];
            for(int i = 0; i < n2; i++) v_total[n1 + i] = v2[i];
            combined[c] = v_total;
            break;
        }
        default:
            stop("Unsupported column type in wl_join_cpp");
        }
    }

    // Sort by referral_index
    IntegerVector order_idx = seq(0, n_total - 1);
    std::sort(order_idx.begin(), order_idx.end(),
              [&](int i, int j) {
                  SEXP col = combined[referral_index];
                  switch(TYPEOF(col)) {
                  case REALSXP:
                      return NumericVector(col)[i] < NumericVector(col)[j];
                  case INTSXP:
                      return IntegerVector(col)[i] < IntegerVector(col)[j];
                  case STRSXP:
                      return CharacterVector(col)[i] < CharacterVector(col)[j];
                  default:
                      stop("Unsupported column type for sorting in wl_join_cpp");
                  }
              });

    // Create sorted DataFrame
    List sorted_cols(n_cols);
    sorted_cols.attr("names") = col_names;
    for(int c = 0; c < n_cols; c++) {
        SEXP col = combined[c];
        switch(TYPEOF(col)) {
        case REALSXP: {
            NumericVector v(col);
            NumericVector tmp(n_total);
            for(int i = 0; i < n_total; i++) tmp[i] = v[order_idx[i]];
            sorted_cols[c] = tmp;
            break;
        }
        case INTSXP: {
            IntegerVector v(col);
            IntegerVector tmp(n_total);
            for(int i = 0; i < n_total; i++) tmp[i] = v[order_idx[i]];
            sorted_cols[c] = tmp;
            break;
        }
        case STRSXP: {
            CharacterVector v(col);
            CharacterVector tmp(n_total);
            for(int i = 0; i < n_total; i++) tmp[i] = v[order_idx[i]];
            sorted_cols[c] = tmp;
            break;
        }
        case VECSXP: {
            List v(col);
            List tmp(n_total);
            for(int i = 0; i < n_total; i++) tmp[i] = v[order_idx[i]];
            sorted_cols[c] = tmp;
            break;
        }
        }
    }

    DataFrame sorted_df(sorted_cols);
    return sorted_df;
}

// wl_schedule_cpp
DataFrame wl_schedule_cpp(
        DataFrame waiting_list,
        DateVector schedule,
        int referral_index = 0,
        int removal_index = 1,
        bool unscheduled = false
) {
    int n = waiting_list.nrows();
    DateVector referral = waiting_list[referral_index];
    DateVector removal = waiting_list[removal_index];

    std::vector<size_t> wl_idx;
    for(size_t i = 0; i < (size_t)n; i++) {
        if(NumericVector::is_na(removal[i])) wl_idx.push_back(i);
    }

    size_t i = 0;
    if(!unscheduled) {
        for(int j = 0; j < schedule.size(); j++) {
            if(i >= wl_idx.size()) break;
            size_t idx = wl_idx[i];
            if(schedule[j] > referral[idx]) {
                removal[idx] = schedule[j];
                i++;
            }
        }
        return DataFrame::create(
            _["Referral"] = referral,
            _["Removal"] = removal
        );
    } else {
        IntegerVector scheduled(schedule.size(), 0);
        for(int j = 0; j < schedule.size(); j++) {
            if(i >= wl_idx.size()) break;
            size_t idx = wl_idx[i];
            if(schedule[j] > referral[idx]) {
                removal[idx] = schedule[j];
                scheduled[j] = 1;
                i++;
            }
        }
        return List::create(
            _["updated_list"] = DataFrame::create(_["Referral"] = referral, _["Removal"] = removal),
            _["scheduled"] = scheduled
        );
    }
}

// [[Rcpp::export]]
DataFrame wl_simulator_cpp(
        Nullable<Date> start_date_ = R_NilValue,
        Nullable<Date> end_date_ = R_NilValue,
        double demand = 10.0,
        double capacity = 11.0,
        DataFrame waiting_list = DataFrame::create(),
        double withdrawal_prob = NA_REAL,
        bool detailed_sim = false
) {
    // Start and end dates
    Date start_date = start_date_.isNotNull() ? as<Date>(start_date_) : as<Date>(Rcpp::Function("Sys.Date")());
    Date end_date = end_date_.isNotNull() ? as<Date>(end_date_) : start_date + 31;

    int number_of_days = end_date - start_date;
    double total_demand = demand * number_of_days / 7.0;
    double daily_capacity = capacity / 7.0;

    // Realized demand and referral dates
    int realized_demand = R::rpois(total_demand);
    DateVector referral(realized_demand);
    for(int i = 0; i < realized_demand; i++) {
        int offset = floor(R::runif(0, number_of_days + 1));
        referral[i] = start_date + offset;
    }
    std::sort(referral.begin(), referral.end());

    // Initialize removal and withdrawal
    DateVector removal(realized_demand);
    DateVector withdrawal(realized_demand);
    std::fill(removal.begin(), removal.end(), Date(NumericVector::get_na()));
    std::fill(withdrawal.begin(), withdrawal.end(), Date(NumericVector::get_na()));

    // Withdrawals
    if(!detailed_sim) {
        if(!NumericVector::is_na(withdrawal_prob)) {
            IntegerVector geom_draw = rgeom_cpp(realized_demand, withdrawal_prob);
            for(int i = 0; i < realized_demand; i++) {
                Date w = referral[i] + geom_draw[i] + 1;
                if(w > end_date) w = Date(NumericVector::get_na());
                withdrawal[i] = w;
            }
        }
    } else {
        if(NumericVector::is_na(withdrawal_prob)) withdrawal_prob = 0.1;
        IntegerVector geom_draw = rgeom_cpp(realized_demand, withdrawal_prob);
        for(int i = 0; i < realized_demand; i++) {
            Date w = referral[i] + geom_draw[i] + 1;
            if(w > end_date) w = Date(NumericVector::get_na());
            withdrawal[i] = w;
        }
    }

    // Construct new waiting list
    DataFrame wl_simulated = DataFrame::create(
        _["Referral"] = referral,
        _["Removal"] = removal,
        _["Withdrawal"] = withdrawal
    );

    // Merge with existing waiting_list if provided
    if(waiting_list.nrows() > 0) {
        wl_simulated = wl_join_cpp(waiting_list, wl_simulated);
    }

    // Schedule patients
    if(daily_capacity > 0) {
        int total_slots = std::ceil(daily_capacity * number_of_days);
        DateVector schedule(total_slots);
        for(int i = 0; i < total_slots; i++) {
            schedule[i] = start_date + (int)(i / daily_capacity);
        }
        wl_simulated = wl_schedule_cpp(wl_simulated, schedule);
    }

    return wl_simulated;
}

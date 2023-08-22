# Required packages
lapply(c("tidyverse", "readxl", "freedom", "truncnorm","data.table", "doParallel", "abind"), require, character.only = T)

# Population model
ahle_sr <- function(
    file, sheet = 1, par_names = "AHLE Parameter", ignore = "Notes", nruns = 1000, Num_months = 12, 
    baseline_scen = NULL, control_scen = NULL, ill_month = NULL
){
  lapply( c("tidyverse", "readxl", "freedom", "truncnorm","data.table", "doParallel", "abind"), require, character.only = T)
  cl = (detectCores() *.94) %>% floor %>% makeCluster
  registerDoParallel(cl)
  age = c("N", "J", "A"); sex = c("F", "M"); cats = c(age, sex) 
  .g = expand_grid(age,sex)
  .t =  paste0(.g$age,.g$sex)      
  .p = paste0(".", .t) %>% as.list
  .l = map(cats, function(x) map(.t, function(y) grepl(x, y) %>% sum))
  names(.l) <- cats %>% tolower %>% paste0(".", .)
  data = read_excel(file, sheet)
  if(is.null(baseline_scen)){
    baseline_scen = names(data) %>% . [ !. %in% c(par_names, ignore)] 
  } else baseline_scen = baseline_scen
  f_scen = function(scen){
    if(is.null(scen)) par = NULL else {
      par = select(data, all_of(c(par_names, scen))) %>% transpose(., make.names = par_names) %>%
        . [ , (colSums(is.na(.))/nrow(.)) < .5] %>% split( . , seq(nrow(.))) %>% 
        map(. , ~ map(.x, ~ eval(parse(text = .x)))) %>% 
        map(. , 
            function(x, name){
              attach(x)
              N0 = list(N_NF_t0, N_NM_t0, N_JF_t0, N_JM_t0, N_AF_t0, N_AM_t0)
              names(N0) <- name
              par = list(
                g = Beta, 
                pt = part, 
                pf = prolif, 
                pfm =	prop_F_milked, 
                ld = lac_duration, 
                ady =	avg_daily_yield_ltr, 
                mvl =	milk_value_ltr, 
                o = list(GammaF, GammaM) %>% rep( 3 ), 
                d	= list(AlphaN, AlphaN, AlphaJ, AlphaJ, AlphaF, AlphaM), 
                c = list(CullF, CullM) %>% rep( 3 ), 
                lw = list(lwNF, lwNM, lwJF, lwJM,lwAF,lwAM), 
                ccy =	ccy, 
                fv = list(fvNF, fvNM, fvJF, fvJM, fvAF, fvAM), 
                hr = hides_rate, 
                hrm =	hides_rate_mor, 
                hv = hides_value, 
                m = list( Man_N, Man_J, Man_A) %>% rep( each = 2), 
                mv = Man_value, 
                dm = list(DM_req_prpn_NF, DM_req_prpn_NM, DM_req_prpn_JF, DM_req_prpn_JM, DM_req_prpn_AF, DM_req_prpn_AM), 
                plp =	prpn_lskeepers_purch_feed, 
                pfp =	prpn_feed_paid_for, 
                fc = Feed_cost_kg, 
                dif =	DM_in_feed, 
                lsr =	Lab_SR, 
                lnh =	lab_non_health, 
                he = Health_exp, 
                ir = Interest_rate, 
                iph	=	Infrastructure_per_head
              )
              detach(x)
              par %>% map( ~ {
                if( is.list(.x) ) .x = .x else .x = list(.x) %>% rep(6)
              } ) %>% c(list(N0 = N0), .)
            } , .p) 
    }; par
  } 
  b_scen = f_scen(baseline_scen)
  c_scen = f_scen(control_scen)
  f_par = function(
    b_scen, c_scen, age, sex, cats, .g, .t, .p, .l, data,
    file, sheet, par_names, ignore, nruns, Num_months, baseline_scen, control_scen, ill_month
  ){
    out = foreach(seed = 1:nruns) %dopar% {
      require(tidyverse)  
      pop <- list()
      pop[[1]] <- b_scen$N0
      for(i in 1:Num_months){
        attach(pop[[i]])
        pre = list(.AF, .AF, .NF, .NM, .JF, .JM)
        post = list(.NF, .NM, .JF, .JM, .AF, .AM)
        detach(pop[[i]])
        if(i %in% ill_month) p = c_scen else p = b_scen 
        pop[[ i+1 ]] <- pmap(
          list(pre = pre, post = post, .t = .t, .p = .p) %>% c(., .l, p), 
          function(pre, post, .t, .p, .n, .j, .a, .f, .m, N0, g, pt, pf, pfm, ld, ady, mvl, 
                   o, d, c, lw, ccy, fv, hr, hrm, hv, m, mv, dm, plp, pfp, fc, dif, lsr, lnh, he, ir, iph){
            set.seed(seed)
            s = function(x) sample(x, 1)
            df = pmap( list( 
              list(pt*pf/12/2, g, g, d, o, c) %>% map(s), 
              c(list(pre) %>% rep(2), list(post) %>% rep(4)),
              list(.n, .j+.a, .n+.j, 1, .j+.a, .a)
            ), \(x,y,z) x*y*z) %>% data.frame
            names(df) <- c("B", "Gi", "Go", "D", "O", "C")
            Df = df %>% mutate(
              sum = rowSums(.), 
              come = B + Gi, 
              leave = sum - come, 
              diff = come - leave, 
              new = post + diff, 
              NOftk = O * (.j + .a) + C * (.a * .m), 
              PopGro = diff, 
              TotNChg = NOftk + PopGro, 
              TotMort = D, 
              QLwKg = new *s(lw), 
              OftkLw = NOftk *s(lw), 
              QMeat = OftkLw *ccy, 
              QManu = new *s(m) *30, 
              QHides = D * (.j + .a) * hrm, 
              QMilk = new * (.a * .f) * s(pt/12) * pfm * ld * s(ady), # change ady to s(ady). Change pt to pt/12 so that monthly. August 2023. AL.
              QWool = 0, 
              CumDM = new * dm * s(lw) * 30, 
              ValOftk = NOftk *s(fv), 
              ValHerd = diff *s(fv), 
              TotVal = ValHerd + ValOftk, 
              ValManu = QManu * mv, 
              ValHides = QHides * hv, 
              ValMilk = QMilk * mvl, 
              ProdVal = TotVal + ValManu + ValHides + ValMilk, 
              FdCost = new * dm * s(lw) * plp * pfp/s(dif) * s(fc) * 30, 
              LbCost = new * s(lsr) * lnh, 
              HthCost = new * s(he), 
              CapCost = ifelse(i == 1, new, 0) * s(fv) * ir, 
              IstCost = ifelse(i == 1, N0, 0) * s(iph), 
              TotExp = FdCost + LbCost + HthCost + CapCost + IstCost, 
              GrsMrg = ProdVal - TotExp, 
              update = new
            ) 
            names(Df) = names(Df) [ - length(Df) ] %>% paste0(., "_", .t) %>% c(., .p); Df
          }) %>% data.frame  
      }
      out = pop %>% .[-1] %>% do.call(rbind, .) 
      prf = names(out) [ 1 : (length(out)/length(.t)) ] %>% gsub( .t [[1]] , "", .)
      tags = c("", "Overall", paste0(cats, "Com")) %>% map(., ~ paste0(prf, .x))
      subtot = pmap(tags, 
                    function(p, o, n, j, a, f, m){
                      df = out %>% select(starts_with(p)) 
                      rs = rowSums(df) 
                      mp = map(.l, function(x) apply(df, 1, function(y) (unlist(x) * y) %>% sum)) %>% data.frame
                      DF = data.frame(df, rs, mp) 
                      names(DF) [ - c(1 : length(.t)) ] <- c(o, n, j, a, f, m)
                      DF
                    }) %>% data.frame 
      xc = subtot %>% select(starts_with(c(".", "new", "QLwKg"))) %>% names
      subtot %>% mutate_if( ! names(.) %in% xc, cumsum)
    }
    ou = map(
      list(mean, sd, min, function(x) quantile(x, .25), median, function(x) quantile(x, .75), max),
      function(x) abind(out, along = 3) %>% apply(., 1:2, x) %>% as.data.frame
    ) 
    ou %>% map(., ~ .x [ Num_months, ] ) %>% do.call(rbind, .) %>%
      mutate(var_name = c("Mean", "StDev", "Min", "Q1", "Median", "Q3", "Max")) %>% 
      transpose(., NA, F, "Item", "var_name") %>% separate(., Item, c("Item", "Group")) %>% 
      mutate_all(
        function(x){
          lab=c("B", "Gi", "Go", "D", "O", "C", "sum", "come", "leave", "diff", "new", 
                "NOftk", "PopGro", "TotNChg", "TotMort", 
                "QLwKg", "OftkLw", "QMeat", "QManu", "QHides", "QMilk", "QWool", 
                "CumDM", "ValOftk", "ValHerd", "TotVal", 
                "ValManu", "ValHides", "ValMilk", "ProdVal", 
                "FdCost", "LbCost", "HthCost", "CapCost", "IstCost", "TotExp", "GrsMrg", 
                "NF", "NM", "JF", "JM", "AF", "AM", 
                "Overall", "NCom", "JCom", "ACom", "FCom", "MCom")
          name=c("Births", "Growth in", "Growth out", "Deaths", "Offtakes", "Culls", "Sum", "Inflow", "Outflow", "Difference", "Population", 
                 "Num Offtake", "Cml Pop Growth", "Total Number Increase", "Total Mortality", 
                 "Population Liveweight (kg)", "Offtake Liveweight (kg)", "Meat (kg)", "Manure", "Hides", "Milk", "Wool", 
                 "Cml Dry Matter", "Value of Offtake", "Value of Herd Increase", "Value of Herd Increase plus Offtake", 
                 "Value of Manure", "Value of Hides", "Value of Milk", "Total Production Value", 
                 "Feed Cost", "Labour Cost", "Health Cost", "Capital Cost", "Infrastructure Cost", "Total Expenditure", "Gross Margin", 
                 "Neonatal Female", "Neonatal Male", "Juvenile Female", "Juvenile Male", "Adult Female", "Adult Male", 
                 "Overall", "Neonatal Combined", "Juvenile Combined", "Adult Combined", "Female Combined", "Male Combined")
          for(i in 1:length(lab)) x[x == lab[[i]]] <- name[[i]]; x
        }
      ) %>% .[.$Item != "",]    
  }
  output =map(b_scen, f_par, c_scen[[1]], age, sex, cats, .g, .t, .p, .l, data, 
              file, sheet, par_names, ignore, nruns, Num_months, baseline_scen, control_scen, ill_month)
  names(output) = baseline_scen
  stopCluster(cl)
  registerDoSEQ()
  output
}

# Helper csv function
csv_to_output <- function(file_name){
  paste0("output/", file_name,   ".csv")
  }
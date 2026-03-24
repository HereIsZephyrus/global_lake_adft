# Introduction to statistical modelingof extreme values

![](images/a38a7234d9325c13bd6f3f5ca77b9679e64a68743952a32b9db8eacae807cf4d.jpg)


![](images/97dc0a1ffe7219ed9d6a7a46f04cd426f54897bde1d21d9272e2b9ec042e7e99.jpg)


# UniversidadZaragoza

# Inés Ortega Palaciosde fin de grado en MatemáticasUniversidad de Zaragoza

Directores del trabajo: Jesús Abaurrea y Ana C.Cebrián

28 de junio de 2017

# Prologue

The general aim of this work is to give a description of extreme value models. These models will beused to analyse the occurrence of floods in the Ebro river in Zaragoza. Different approaches have beenconsidered in order to express the distribution of these extremes.

The first studies in extreme value theory were written in the first half of the 20th century. An articlewritten by L.H.C. Tippett and R.A. Fisher in 1928 and the extended theory that B.V. Gnedenko wrotein 1948 based on their work were the first results in this area. In the second half of the same century,E.J. Gumbel captured the first statistical applications in his book Statistics of extremes (1958) [9] anddescribed the Gumbel distribution to model rare physical phenomena. Other remarkable publicationsused in this work are Extremes and related properties of random sequences and processes by M.R.Leadbetter [13] and An introduction to statistical modeling of extreme values by S. Coles [3]. Thespecific objective of this work is the study of the distributions of extremes in order to apply them in apractical case.

The classical extreme value models define extremes as maximum (or minimum) values per unit oftime. The class of distributions described in the first chapter is a 3-parameter distribution called thegeneralized extreme value (GEV) distribution.

The "Excess over threshold" method suggests that an extreme value is an observation which exceedsa concrete threshold. The choice of this threshold can be challenging depending on the data set. Thesecond chapter shows the limit distribution which is fitted to these extreme values, which is called thegeneralized Pareto distribution.

The occurrence of excesses over a fixed threshold follows a Poisson process. A characterization ofa Poisson process and its relation with both GEV and Pareto distributions are given in the third chapter.

An important consideration is the condition for which the extreme distributions can be fitted todependent sequences of observations. A cluster process is considered to represent the occurrences.

In order to predict floods in the Ebro river in Zaragoza, estimations of distributions of both modelsand return levels are calculated in the last chapter. The statistical programming language R is the maintool used in this work in order to estimate these extreme value models and their return levels for thisdata set.

# Resumen

La teoría de valores extremos es una rama de la estadística que centra su interés en el comporta-miento de los valores más altos (o más bajos) de la variable a estudiar.

Los primeros resultados de la teoría de valores extremos datan de la primera mitad del siglo XX.Los artículos expuestos por L. Tippett y R.A. Fisher en 1928 fueron pioneros en el área y en 1958,E.J. Gumbel plasmó esta teoría en su libro Statistics of extremes [9] en el que incluía la distribuciónde valores extremos que hoy en día lleva su nombre. En la actualidad, la teoría de valores extremos esaplicada en muchos campos, como la hidrología, el análisis de riesgos en finanzas o la geología.

El primer enfoque que se considera en este trabajo al describir valores extremos es el análisis demáximos. Sea una sucesión de variables aleatorias independientes $X _ { 1 } , \ldots , X _ { n }$ con la misma función dedistribución $F$ . Teóricamente, la distribución de $M _ { n } = \operatorname* { m a x } ( X _ { 1 } , \ldots , X _ { n } )$ se puede obtener de la forma

$$
\wp (M _ {n} \leq z) = \wp (X _ {1} \leq z) \times \dots \times \wp (X _ {n} \leq z) = (F (z)) ^ {n}
$$

Usualmente, la distribución $F$ es desconocida. El teorema de Fisher-Tippet-Gnedenko, desarrolladopor los dos primeros en 1928 y probado por el tercero en 1943, permite obtener una aproximaciónde la distribución de máximos, $M _ { n }$ . Dadas dos sucesiones de constantes $\{ a _ { n } > 0 \}$ y $\left\{ b _ { n } \right\}$ tales que$\begin{array} { r } { \operatorname* { l i m } _ { n  \infty } \wp ( ( M _ { n } - b _ { n } ) / a _ { n } \leq z ) = G ( z ) } \end{array}$ con $G ( z )$ función de distribución no degenerada, entonces $G$ es dela forma

$$
G (z) = \exp \left(- \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}\right)
$$

con $\left\{ z : 1 + \xi ( z - \mu ) / \sigma > 0 \right\}$ , $- \infty < \mu < \infty$ , $\sigma > 0$ y −∞ < ξ < ∞.

Esta distribución es conocida como "Distribución de Valores Extremos Generalizada (VEG)". Dadoun conjunto de datos en un caso práctico, se puede ajustar la VEG a la serie de máximos del conjunto,generalmente anuales, y así obtener estimaciones de los parámetros de la distribución.

El segundo enfoque considerado en el trabajo es la descripción de valores extremos de un conjuntode datos como aquellas observaciones que superen un umbral fijo. Este método se denomina "Métodode excesos sobre umbral". Las dos distribuciones que se deben ajustar en este caso son la distribucióndel número de ocurrencias sobre el umbral en un periodo de tiempo y la distribución de los excesossobre umbral.

Dada una variable aleatoria $X$ con función de distribución desconocida $F$ , la distribución de excesossobre un umbral $u$ se describe como $F _ { u } ( x ) = \wp \{ X - u \leq x | X > u \}$ con $x \ge 0$ .

Utilizando los resultados de la teoría del análisis de máximos, $F ^ { n }$ puede ajustarse por una VEG yasí se puede obtener la distribución asintótica de los excesos sobre el umbral:

$$
H (y) = 1 - \left(1 + \frac {\xi y}{\sigma_ {u}}\right) ^ {- 1 / \xi}
$$

definida en $\{ y : y > 0$ , $\left( 1 + \xi y / \sigma _ { u } > 0 \right) \}$ con $\boldsymbol { \sigma _ { u } } = \boldsymbol { \sigma } + \boldsymbol { \xi } \left( \boldsymbol { u } - \boldsymbol { \mu } \right)$ . Esta distribución se denomina "Paretogeneralizada".

Antes de ajustar una distribución Pareto a un conjunto de datos se debe fijar un umbral. Su elecciónpuede ser complicada, ya que si se elige un umbral demasiado alto, el número de excesos no sería

suficiente para ajustar la distribución y un umbral demasiado bajo proporcionaría un gran número deexcesos de los cuales no todos serían valores extremos reales. Utilizando una gráfica de vida mediaresidual, que relaciona el valor del umbral con la media de los valores de los excesos para ese umbral,se obtiene un procedimiento para seleccionar u en los puntos donde la gráfica es lineal.

En este trabajo se estudia cómo se pueden hacer estimaciones del número de ocurrencias y com-probar que estos resultados guardan una estrecha relación con la estimación de los parámetros en ladistribución de VEG y la distribución Pareto generalizada.

Al considerar la aplicación de la teoría de valores extremos a casos prácticos, se debe tener encuenta que ésta supone en todo momento la independencia de las variables a estudio y en la realidadla independencia no es habitual. En el tercer capítulo se explican las condiciones que debe cumplir unconjunto de datos para que se puedan ajustar las distribuciones de extremos. Es suficiente con ver quesi la distribución tiene un carácter estacionario y no tiene dependencia a largo plazo, muchos de losresultados para series independientes pueden aplicarse en este caso.

En el cuarto capítulo se considera el conjunto de datos de niveles de agua diarios, medidos enmetros, del Río Ebro a su paso por Zaragoza desde 1961 a 2016. Por sus condiciones físicas, el niveldel río presenta dependencia a corto plazo. Por lo tanto, es correcto aplicar en este caso la teoría devalores extremos expuesta en el trabajo para modelizar la ocurrencia de riadas en Zaragoza. Utilizandoel lenguaje de programación estadística R, se ajustan una distribución VEG y una distribución Paretogeneralizada de modo que se obtienen probabilidades de niveles de retorno de la altura del agua.

# Contents

# Prologue III

# Resumen V

# 1. Classical extreme value theory and models 1

1.1. Asymptotic Models 1

1.1.1. Model Formulation 1

1.1.2. Extremal Types Theorem and its generalization 2

1.1.3. Return levels 4

1.1.4. General distribution for minima 4

1.2. Inference for the GEV Distribution 5

1.2.1. Maximum likelihood estimation 5

1.2.2. Inference for return levels 5

# 2. Threshold Models 7

2.1. Asymptotic Model Characterization 7

2.1.1. The Generalized Pareto Distribution: Description and justification . . 7

2.1.2. Examples . 9

2.2. Modeling Threshold Excesses 9

2.2.1. Methods of Threshold Selection 9

2.2.2. Parameter and return level estimation 10

2.2.3. Model checking . . 12

# 3. Point processes 13

3.1. Point processes of exceedances . 13

3.1.1. Poisson process and some definitions 13

3.1.2. Convergence law of point processes . . 14

3.2. Relation with other extreme value models 14

3.2.1. Relation with the GEV family of distributions . . 14

3.2.2. Relation with generalized Pareto family of distributions . 15

3.3. Estimations of the Poisson process . 15

3.3.1. Estimation of parameters . . 15

3.3.2. Estimation of return levels 16

3.4. Extremes on dependent sequences 16

# 4. Practical example: extreme distributions of water levels in the Ebro river in Zaragoza 19

4.1. Data and exploratory analysis . . 19

4.2. Extreme behaviour and extreme distributions of data 20

4.2.1. GEV approximation 20

4.2.2. GPD approximation 21

A. R Code 23

A.1. Data and exploratory analysis Code 23

A.2. GEV approximation Code 23

A.3. GPD approximation Code . 24

Bibliography 27

Index 29

# Chapter 1

# Classical extreme value theory and models

The statistical behaviour of the distribution of maxima of independent random variables with acommon distribution function is the main objective of this chapter. Thus, basic ideas of extreme valuetheory and its models will be presented.

The central result is the Fisher-Tippett-Gnedenko Theorem, which describes the form of the limitdistribution of the normalised maxima. The three models obtained in this result form the GeneralizedExtreme Value family of distributions which will be explained in detail in Section 1.1.2. Another con-sideration is the characterisation of the model for minima and the analytical solution that can be foundfor return levels (Section 1.1.3).

The Generalized Extreme Value parameters and return levels will be estimated using the maximumlikelihood estimation method.

# 1.1. Asymptotic Models

# 1.1.1. Model Formulation

Let $X _ { 1 } , \cdots , X _ { n }$ be independent identically distributed random variables and let

$$
M _ {n} = \max  \left(X _ {1}, \dots , X _ {n}\right) \tag {1.1}
$$

Let $F$ be the distribution function of $X _ { 1 } , \cdots , X _ { n }$ . The distribution function of $M _ { n }$ can be calculatedexactly for all values of n using the independence of the random variables as follows:

$$
\wp (M _ {n} \leq z) = \wp (X _ {1} \leq z, \dots , X _ {n} \leq z) = \wp (X _ {1} \leq z) \times \dots \times \wp (X _ {n} \leq z) = (F (z)) ^ {n}
$$

Although this may seem useful, there is a problem when calculating this if the distribution function$F$ is unknown.

When $F$ is estimated, it is clear that there can be little discrepancies due to the lack of data. In thiscase, some calculations are needed to obtain an estimation of $F ^ { n }$ , but the $n ^ { t h }$ -power of a little error leadsto a problem with huge discrepancies. Thus, in order to get the distribution function of maxima, $F ^ { n }$ ,another way to solve the problem is to assume that $F$ is unknown and try to estimate $F ^ { n }$ using observedextreme data.

Now, let the behaviour of $F ^ { n }$ when $n \to \infty$ be considered. As $F$ is a distribution function, let $z _ { + }$ be theupper-end point of $F$ , that is, $z _ { + } = \operatorname* { s u p } \left\{ x \in \mathbb { R } : F ( x ) < 1 \right\}$ . Then the following equality is immediatelyobtained

$$
\forall z <   z _ {+}, \wp (M _ {n} \leq z) = F ^ {n} (z) \rightarrow 0 \text {w h e n} n \rightarrow \infty
$$

and in the case $z _ { + } < \infty$ , for any $z \geq z _ { + }$

$$
\wp (M _ {n} \leq z) = F ^ {n} (z) = 1
$$

Thus, the distribution of $M _ { n }$ degenerates to a point mass on $z _ { + }$

The difficulty of calculating $M _ { n }$ can be partly saved by giving a linear normalization of $M _ { n }$ . With anappropriate choice of two sequences of constants, $\left( a _ { n } > 0 \right)$ and $\left( b _ { n } \right)$ ,

$$
M _ {n} ^ {*} = \frac {M _ {n} - b _ {n}}{a _ {n}}
$$

and this allows to study the behaviour of $M _ { n } ^ { * }$ instead of $M _ { n }$ , which will be useful in order to explain thegeneral extreme value theory.

# 1.1.2. Extremal Types Theorem and its generalization

Some results, based on [13, Section 1.4], need to be considered before giving an accurate definitionof the Extremal Types theorem.

Definition 1.1. A distribution function $G$ is said to be max-stable if, for every $n \in \mathbb { N } , n > 1$ , there areconstants $a _ { n } > 0$ and $b _ { n }$ such that

$$
G ^ {n} \left(a _ {n} z + b _ {n}\right) = G (z)
$$

From this concept and in order to continue with the results, several definitions of the domain ofattraction can be given. Quoting [14],

Definition 1.2. Let $X _ { 1 } , \ldots , X _ { n }$ be mutually independent random variables with common distributionfunction $F ( z )$ and $M _ { n }$ the maximum of these random variables. Suppose there exist a pair of sequences$\left( a _ { n } > 0 \right)$ and $\left( b _ { n } \right)$ and a distribution function $G ( z )$ such that

$$
\lim  _ {n \rightarrow \infty} \wp \left\{\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z \right\} = \lim  _ {n \rightarrow \infty} F ^ {n} \left(a _ {n} z + b _ {n}\right) = G (z)
$$

for all z at which $G ( z )$ is continuous. Then $F ( z )$ lies in the domain of attraction of G , $F \in D ( G )$ .

Theorem 1.3. (i) A non-degenerate distribution function $G$ is max-stable if and only if there is a se-quence $\left\{ F _ { n } \right\}$ of distribution functions and constants $a _ { n } > 0$ and $b _ { n }$ such that

$$
F _ {n} \left(a _ {n k} ^ {- 1} z + b _ {n k}\right)\rightarrow G ^ {1 / k} (z) \tag {1.2}
$$

as $n \to \infty$ for each $k = 1 , 2 , \cdots$

(ii) In particular, if G is non-degenerate, $D ( G )$ is non-empty if and only if G is max-stable. Then also$G \in D ( G )$

The following theorem contains the most important result about extreme value distribution func-tions. Fisher and Tippett started the research in 1928, and later, Gnedenko formalized it in 1948.

Theorem 1.4 (Fisher - Tippett - Gnedenko theorem). If there exist sequences of constants $\left( a _ { n } > 0 \right)$ ) and$\left( b _ { n } \right)$ such that

$$
\lim  _ {n \rightarrow \infty} \wp \left(\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z\right) = G (z)
$$

where $G$ is a non-degenerate distribution function, then G belongs to one of the following families

I $\begin{array} { r } { \mathbf { \tilde { \mathbf { \Gamma } } } : G ( z ) = \exp \left( - \exp \left( - \left( \frac { z - b } { a } \right) \right) \right) } \end{array}$ , $- \infty < z < \infty$ (Gumbel distribution)

$$
I I: G (z) = \left\{ \begin{array}{l l} 0 & z \leq b \\ \exp \left(- \left(\frac {z - b}{a}\right) ^ {- \alpha}\right) & z > b \end{array} \right. (F r e c h e t d i s t r i b u t i o n)
$$

$$
I I I: G (z) = \left\{ \begin{array}{l l} \exp \Big (- \Big (- \Big (\frac {z - b}{a} \Big) ^ {\alpha} \Big) \Big) & z <   b \\ 1 & z \geq b \end{array} \right. (W e i b u l l d i s t r i b u t i o n)
$$

for a scale parameter $a > 0$ , a location parameter b and a shape parameter $\alpha > 0$

This theorem shows that the only possible limiting distribution for $M _ { n } ^ { * }$ given $M _ { n }$ and sequences$\left( a _ { n } > 0 \right)$ and $\left( b _ { n } \right)$ , is one of these three types. In some way, this theorem gives an extreme valueanalogue of the central limit theorem, as $M _ { n } ^ { * }$ is the normalized distribution of $M _ { n }$ .

By reformulating the three models in Theorem 1.4, they can be combined into a single family ofmodels as follows:

Theorem 1.5. If there exist sequences of constants $( a _ { n } > 0 )$ ) and $\left( b _ { n } \right)$ such that

$$
\lim  _ {n \rightarrow \infty} \wp \left(\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z\right) = G (z)
$$

for a non-degenerate distribution function, G, then G is a member of the family

$$
G (z) = \exp \left(- \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}\right) \tag {1.3}
$$

defined on the set: $\left\{ z : 1 + \xi ( z - \mu ) / \sigma > 0 \right\}$ with location parameter $- \infty < \mu < \infty$ , scale parameter$\sigma > 0$ and shape parameter $- \infty < \xi < \infty$ . From now on, this family will be called the generalizedextreme value (GEV) family of distributions.

Note 1.6. The model (1.3) has a location parameter, µ; a scale parameter, $\sigma$ ; and a shape parameter,$\xi$ . Observe that given the three distributions in Theorem 1.4, type II corresponds to the case $\xi > 0$ ;type III to the case $\xi < 0$ and the Gumbel distribution to the GEV family’s subset with $\xi = 0$

The notation in Theorem 1.4 can be simplified. Before giving a new version of the theorem, thefollowing equivalence relation is defined.

Definition 1.7. Two distributions $F$ and $F ^ { * }$ are said to be of the same type if there exist constants a andb such that $F ^ { * } ( a z + b ) = F ( z )$ for all $z$ .

Theorem 1.8. Every max-stable distribution is of extreme value type and equal to $G ( a z + b )$ for some$a > 0$ and b where for

$$
\begin{array}{l} I: G (z) = \exp (- \exp (- z)), - \infty <   z <   \infty \\ I I \colon G (z) = \left\{ \begin{array}{l l} 0 & z \leq 0 \\ \exp (- z ^ {- \alpha}) & z > 0 \end{array} \right. \\ I I I: G (z) = \left\{ \begin{array}{l l} \exp \left(- (- z) ^ {\alpha}\right) & z <   0 \\ 1 & z \geq 0 \end{array} \right. \\ \end{array}
$$

Conversely, each distribution of extreme value type is max-stable.

With all these results a brief proof of the theorem can be given.

Theorem 1.9. Let $M _ { n }$ be as defined in (1.1), then for some constants $a _ { n } > 0$ and $b _ { n }$ it satisfies

$$
\wp \left(\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z\right)\rightarrow G (z) \tag {1.4}
$$

for some non-degenerate G if and only if G is one of the three extreme value type distributions definedin Theorem 1.8.

Proof. If (1.4) is true, then Theorem 1.3 shows that $G$ has to be max-stable, and as it is shown inTheorem 1.8 is of the extreme value type. Conversely, if $G$ is an extreme value function type, inparticular, it is max-stable, and Theorem 1.3 shows that $G \in D ( G )$ and the result holds. □

# 1.1.3. Return levels

Quantiles can be obtained by finding the inverse of (1.3).

In common terminology, $z _ { p }$ is defined as the return level associated to the return period $1 / p$ . Theprobability of the occurrence of $z _ { p }$ is $p$ and once every $1 / p$ years the annual maximum is expected to begreater than $z _ { p }$ .

The return level $z _ { p }$ is exceeded by the annual maximum in a particular year with probability $p$ , thatis, $z _ { p }$ is expected to be exceeded by the annual maximum once every $1 / p$ years.

Given some data series, they can be grouped in packages of n observations, for a large value n, andgenerate a series of block maxima. Often they are chosen to correspond to a time period, for exampleone year. Using this information, this series can be fitted to a GEV distribution.

In order to estimate the return level, the general equation of the GEV distribution leads to,

$$
1 - p = \exp \left(- \left(1 + \xi \left(\frac {z _ {p} - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}\right) \Rightarrow 1 + \xi \left(\frac {z _ {p} - \mu}{\sigma}\right) = [ - \log (1 - p) ] ^ {- \xi}
$$

and therefore

$$
z _ {p} = \left\{ \begin{array}{l l} \mu - \frac {\sigma}{\xi} \left[ 1 - \left\{- \log (1 - p) \right\} ^ {- \xi} \right] & \xi \neq 0 \\ \mu - \sigma \log \left\{- \log (1 - p) \right\} & \xi = 0 \end{array} \right. \tag {1.5}
$$

Note 1.10. When plotting $z _ { p }$ against $- \log \left( 1 - p \right)$ on a logarithmic scale three cases be can distin-guished depending on the value of $\xi$ . $L e t y _ { p } = - \log ( 1 - p )$ . Then, $z _ { p }$ can be plotted against $x _ { p } = l o g ( y _ { p } )$so,

■ $I f \xi = 0$ , the following expression holds,

$$
z _ {p} = \mu - \sigma \log \left\{- \log (1 - p) \right\} = \mu - \sigma \log (y _ {p}) = \mu - \sigma x _ {p}
$$

Thus, the plot is linear.

$I f \xi \neq 0$ , $z _ { p }$ satisfies

$$
\mu - \frac {\sigma}{\xi} \left[ 1 - \left\{- \log (1 - p) \right\} ^ {- \xi} \right] = \mu - \frac {\sigma}{\xi} \left[ 1 - y _ {p} ^ {- \xi} \right] = \mu - \frac {\sigma}{\xi} [ 1 - \exp (- \xi x _ {p}) ]
$$

• If $\xi < 0$ , the plot is convex and it has its asymptotic limit when $p  0$ at $\textstyle \mu - { \frac { \sigma } { \xi } }$

• If $\xi > 0$ , the plot is concave and has not finite bound.

This graph is called the return level plot.

# 1.1.4. General distribution for minima

As some applications may need a model of minima instead of maxima, some transformations can beapplied to the previous results in order to give an expression of the distribution of $\tilde { M } _ { n } = m i n \{ X _ { 1 } , \ldots , X _ { n } \}$ ,where the $X _ { i }$ are independent random variables with a common distribution function.

Let $Y _ { i } = - X _ { i }$ for $i = 1 , \ldots , n$ . Each large value of $Y _ { i }$ corresponds to the small value of $X _ { i }$ so if$\tilde { M } _ { n } = m i n \{ X _ { 1 } , \ldots , X _ { n } \}$ and $M _ { n } = m a x \{ Y _ { 1 } , \ldots , Y _ { n } \}$ it is easy to see that $\tilde { M } _ { n } = - M _ { n }$ and for a large n,

$$
\wp \left\{\tilde {M} _ {n} \leq z \right\} = \wp \{- M _ {n} \leq z \} = \wp \{M _ {n} \geq - z \} = 1 - \wp \{M _ {n} \leq - z \} \approx
$$

$$
1 - \exp \left(- \left(1 + \xi \left(\frac {- z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}\right) = 1 - \exp \left(- \left(1 - \xi \left(\frac {z - \tilde {\mu}}{\sigma}\right)\right) ^ {- 1 / \xi}\right)
$$

on $\{ z : 1 - \xi ( z - \tilde { \mu } ) / \sigma > 0 \}$ where ${ \tilde { \mu } } = - \mu$ . This distribution is the GEV distribution for minima.Therefore, in a similar way to Theorem 1.5, there is a theorem for the distribution of minima ([3,Theorem 3.3]).

Theorem 1.11. If there exist sequences of constants $\{ a _ { n } > 0 \}$ and $\left\{ b _ { n } \right\}$ such that

$$
\wp \left(\frac {\tilde {M} _ {n} - b _ {n}}{a _ {n}} \leq z\right) \to \tilde {G} (z)
$$

as $n  \infty$ for a non-degenerate distribution function, ${ \tilde { G } } _ { \mathrm { { } } }$ , then $\tilde { G }$ is a member of the GEV family ofdistributions for minima where $- \infty < \mu < \infty$ , $\sigma > 0$ and $- \infty < \xi < \infty$ .

# 1.2. Inference for the GEV Distribution

# 1.2.1. Maximum likelihood estimation

Let $Z _ { 1 } , \ldots , Z _ { n }$ be independent random variables with the GEV distribution function and suppose that$\xi \neq 0$ . Knowing that the general distribution function is defined as in Theorem 1.5, the density functionof a random variable $Z$ with parameters $( \mu , \sigma , \xi )$ can be obtained as follows,

$$
f (\mu , \sigma , \xi | z) = \frac {d}{d z} G (z) = \frac {1}{\sigma} \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 - 1 / \xi} - \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}
$$

Thus, the log-likelihood of $Z _ { 1 } , \ldots , Z _ { n }$ can be calculated using the definition,

$$
\begin{array}{l} l \left(\mu , \sigma , \xi | Z _ {1}, \dots , Z _ {n}\right) = \log \left(\prod_ {i = 1} ^ {n} f (\mu , \sigma , \xi | Z _ {i})\right) = \sum_ {i = 1} ^ {n} \log f (\mu , \sigma , \xi | Z _ {i}) = \\ = \sum_ {i = 1} ^ {n} \log \left[ \frac {1}{\sigma} \left(1 + \xi \left(\frac {Z _ {i} - \mu}{\sigma}\right)\right) ^ {- 1 - 1 / \xi} - \left(1 + \xi \left(\frac {Z _ {i} - \mu}{\sigma}\right)\right) ^ {- 1 / \xi} \right] = \\ = - m \log \sigma - \left(1 + \frac {1}{\xi}\right) \sum_ {i = 1} ^ {n} \log \left[ 1 + \xi \left(\frac {Z _ {i} - \mu}{\sigma}\right) \right] - \sum_ {i = 1} ^ {n} \left[ 1 + \xi \left(\frac {Z _ {i} - \mu}{\sigma}\right) \right] ^ {- 1 / \xi} \tag {1.6} \\ \end{array}
$$

where the parameters $( \mu , \sigma , \xi )$ satisfy

$$
1 + \xi \left(\frac {Z _ {i} - \mu}{\sigma}\right) > 0, f o r i = 1, \dots , n
$$

In the case $\xi = 0$ , the Gumbel limit of the GEV distribution shall be used obtaining

$$
\begin{array}{l} l (\mu , \sigma | Z _ {1}, \dots , Z _ {n}) = \sum_ {i = 1} ^ {n} \log \left(\frac {1}{\sigma} \exp \left\{- \left(\frac {Z _ {i} - \mu}{\sigma}\right) \right\} \exp \left\{- \exp \left\{- \left(\frac {Z _ {i} - \mu}{\sigma}\right) \right\} \right\}\right) = \\ = - m \log \sigma - \sum_ {i = 1} ^ {n} \left(\frac {Z _ {i} - \mu}{\sigma}\right) - \sum_ {i = 1} ^ {n} \exp \left\{- \left(\frac {Z _ {i} - \mu}{\sigma}\right) \right\} \tag {1.7} \\ \end{array}
$$

There is no analytical solution for the maximization of (1.6) and (1.7), but a numerical solution canbe obtained by using standard numerical algorithms.

# 1.2.2. Inference for return levels

Once the maximum likelihood estimators $( \tilde { \mu } , \tilde { \sigma } , \tilde { \xi } )$ of the GEV distribution are calculated, they canbe substituted in order to estimate the maximum likelihood estimator of $z _ { p }$ for $0 < p < 1$ $( 1 / p$ returnlevel). Using (1.5),

$$
\tilde {z} _ {p} = \left\{ \begin{array}{l l} \tilde {\mu} - \frac {\tilde {\sigma}}{\tilde {\xi}} \left[ 1 - y _ {p} ^ {- \tilde {\xi}} \right] & \tilde {\xi} \neq 0 \\ \tilde {\mu} - \tilde {\sigma} \log y _ {p} & \tilde {\xi} = 0 \end{array} \right.
$$

where $y _ { p } = - \log \left( 1 - p \right)$ . An approximation of the variance of $\tilde { z } _ { p }$ can be found by using the Deltamethod.

Theorem 1.12 (Delta method). Suppose that $\mathbb { X } = ( X _ { 1 } , \ldots , X _ { k } )$ is a random vector satisfying

$$
\sqrt {n} (\mathbb {X} - \boldsymbol {\mu}) \stackrel {d} {\to} N (0, \Sigma)
$$

where Σ is the covariance matrix. Let $h : \mathbb { R } ^ { k }  \mathbb { R }$ be a differentiable function and let

$$
\nabla h (\mathbb {X}) = \left( \begin{array}{c} \frac {\partial h}{\partial X _ {1}} \\ \vdots \\ \frac {\partial h}{\partial X _ {k}} \end{array} \right) \tag {1.8}
$$

then

$$
\sqrt {n} \left[ h (\mathbb {X}) - h (\mu) \right] \xrightarrow {d} N \left(0, \nabla h (\boldsymbol {\mu}) ^ {T} \cdot \boldsymbol {\Sigma} \cdot \nabla h (\boldsymbol {\mu})\right)
$$

Therefore, by applying the Delta method, an approximation of $V a r ( h ( \mathbb { X } ) )$ can be found by using thecovariance matrix $\Sigma$ .

Hence, an approximation of the variance of $\tilde { z } _ { p }$ is given by

$$
V a r (\tilde {z} _ {p}) = \nabla z _ {p} ^ {T} \cdot V \cdot \nabla z _ {p} ^ {T}
$$

where V is the variance-covariance matrix of $( \tilde { \mu } , \tilde { \sigma } , \tilde { \xi } )$ and

$$
\nabla z _ {p} ^ {T} = \left[ \frac {\partial z _ {p}}{\partial \mu}, \frac {\partial z _ {p}}{\partial \sigma}, \frac {\partial z _ {p}}{\partial \xi} \right] = \left[ 1, - \xi^ {- 1} \left(1 - y _ {p} ^ {- \xi}\right), \sigma \xi^ {- 2} \left(1 - y _ {p} ^ {- \xi}\right) - \sigma \xi^ {- 1} y _ {p} ^ {- \xi} \log y _ {p} \right]
$$

evaluated at $( \tilde { \mu } , \tilde { \sigma } , \tilde { \xi } )$ .

# Chapter 2

# Threshold Models

Threshold models are based on the study of the limit distribution of exceedances over a fixedthreshold: the generalized Pareto distribution is the distribution which models the behaviour of thesevariables.

An important decision in order to obtain a good extreme value behaviour of excesses is the choiceof the appropriate threshold. In this chapter, two intuitive methods to find a valid threshold will beexplained, as well as an analytical solution to the return levels for the Pareto distribution. Estimators ofparameters and return levels will be obtained using the maximum likelihood estimation method.

The form of the probability and quantile plots of the model against density and return level will beshown, as these plots are useful to evaluate the quality of a fitted generalized Pareto distribution.

# 2.1. Asymptotic Model Characterization

Let $X _ { 1 } , X _ { 2 } , \cdots$ be a sequence of independent random variables with common marginal distributionfunction, $F$ . It can be intuitive to say that $X _ { i }$ is an extreme event if it exceeds some fixed threshold, $u$ . Atheoretical description for this behaviour can be given by the following conditional probability.

Definition 2.1 (Excess distribution function). Let X be a random variable with distribution function F.For a fixed u,

$$
F _ {u} (x) = \wp \{X - u \leq x | X > u \}, x \geq 0 \tag {2.1}
$$

is the excess distribution function of the random variable X over the threshold u.

Using this definition we have the following equality,

$$
\wp \{X > u + x | X > u \} = \frac {1 - F (u + x)}{1 - F (u)} \tag {2.2}
$$

If the distribution function $F$ was known there would be no problem in calculating that probability andobtaining a formula for the distribution. As this is usually not the case, under the same conditions wherethe GEV distribution function can be used as an approximation to the distribution function for maximaof long sequences, an explicit expression of (2.1) can be obtained by substituting the distribution givenin Theorem 1.5 for the distribution function for maxima, $F ^ { n }$ .

# 2.1.1. The Generalized Pareto Distribution: Description and justification

The main Pareto distribution result is given in the following theorem.

Theorem 2.2 (Pickands (1975), Balkema and de Haan (1974)). Let $X _ { 1 } , X _ { 2 } , \cdots$ be a sequence of inde-pendent random variables with common distribution function $F$ , and let

$$
M _ {n} = \max  \{X _ {1}, \dots , X _ {n} \}.
$$

Denote an arbitrary element in the sequence as $X$ , and suppose that $F$ satisfies Theorem 1.5 , then forlarge enough u, the distribution function of $\left( X - u \right)$ , conditional on $X > u$ , is approximately

$$
H (y) = 1 - \left(1 + \frac {\xi y}{\sigma_ {u}}\right) ^ {- 1 / \xi} \tag {2.3}
$$

defined on $\{ y : y > 0$ , $\left( 1 + \xi y / \sigma _ { u } > 0 \right) \}$ where $\boldsymbol { \sigma _ { u } } = \boldsymbol { \sigma } + \boldsymbol { \xi } \left( \boldsymbol { u } - \boldsymbol { \mu } \right)$ .

This distribution is called the generalized Pareto distribution (GPD) where $\sigma _ { u }$ is the scale parameterand $\xi$ is the shape parameter.

Proof. Assuming that Theorem 1.5 is true, for large n,

$$
F ^ {n} (z) \approx e x p \left\{- \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi} \right\}
$$

for some parameters $\mu , \sigma > 0$ and $\xi$ . Taking a logarithmic scale,

$$
n \log F (z) \approx - \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi} \tag {2.4}
$$

Note that for large enough n, a logarithmic expression can be approximated by its Taylor expansion.That implies:

$$
\log (1 + z) = \sum_ {n = 1} ^ {\infty} \frac {(- 1) ^ {n + 1}}{n} z ^ {n} = z - \frac {z ^ {2}}{2} + \frac {z ^ {3}}{3} - \frac {z ^ {4}}{4} + \dots
$$

which means that $\log F ( z ) \approx - \left( 1 - F ( z ) \right)$ . Replacing the approximation in (2.4),

$$
n (1 - F (u)) \approx \left(1 + \xi \left(\frac {u - \mu}{\sigma}\right)\right) ^ {- 1 / \xi} \Longrightarrow 1 - F (u) \approx \frac {1}{n} \left(1 + \xi \left(\frac {u - \mu}{\sigma}\right)\right) ^ {- 1 / \xi} \tag {2.5}
$$

Equivalently, for $y > 0$ ,

$$
1 - F (u + y) \approx \frac {1}{n} \left(1 + \xi \left(\frac {u + y - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}
$$

Hence,

$$
\begin{array}{l} \wp \left\{X > u + y \mid X > u \right\} \approx \frac {n ^ {- 1} \left[ 1 + \xi (u + y - \mu) / \sigma \right] ^ {- 1 / \xi}}{n ^ {- 1} \left[ 1 + \xi (u - \mu) / \sigma \right] ^ {- 1 / \xi}} = \\ = \left[ 1 + \frac {\xi (u + y - \mu) / \sigma}{1 + \xi (u - \mu) / \sigma} \right] ^ {- 1 / \xi} = \left[ 1 + \frac {\xi y}{\sigma_ {u}} \right] ^ {- 1 / \xi} \tag {2.6} \\ \end{array}
$$

where $\boldsymbol { \sigma _ { u } } = \boldsymbol { \sigma } + \boldsymbol { \xi } \left( \boldsymbol { u } - \boldsymbol { \mu } \right)$

□

Note 2.3. If $\xi < 0$ , the distribution of excesses has an upper bound for $u - \sigma _ { u } / \xi$ but if $\xi > 0$ , thedistribution has not an upper limit. For $\xi = 0$ , a distribution approximation can be obtained by takingthe limit $\xi  0$ . Then,

$$
H (y) = 1 - \exp \left(- \frac {y}{\sigma_ {u}}\right) \tag {2.7}
$$

# 2.1.2. Examples

Given a distribution function, $F$ , the Pareto distribution can be calculated as follows:

Example 2.4. For $X \sim U ( 0 , 1 )$ , the distribution function $F ( z ) = z$ , where $z \in [ 0 , 1 ]$ . So,

$$
\wp \left\{X > u + y \mid X > u \right\} = \frac {1 - F (u + y)}{1 - F (u)} = \frac {1 - (u + y)}{1 - u} = 1 - \frac {y}{1 - u}
$$

for $0 \leq y \leq 1 - u$ . This is a generalized Pareto distribution with $\sigma _ { u } = 1 - u$ and $\xi = - 1$

Example 2.5. Given $X \sim \mathrm { e x p } ( 1 )$ , the distribution function is of the form $F ( x ) = 1 - e ^ { - x } f o r x > 0 .$ $x > 0$ . Usingthe definition of Pareto distribution,

$$
\wp \left\{X > u + y \mid X > u \right\} = \frac {1 - F (u + y)}{1 - F (u)} = \frac {e ^ {- (u + y)}}{e ^ {- u}} = e ^ {- y}
$$

for $y > 0$ . Thus, it corresponds to the generalized Pareto distribution with $\xi = 0$ and $\sigma _ { u } = 1$

The GEV distribution describes the limit distribution of normalised maxima while the GPD con-siders the limit distribution of excesses over thresholds using the GEV distribution as the approximationof the distribution of the maxima. Actually, the value of $\xi$ is common across the two models and thevalue of $\sigma$ is threshold-dependent, except in the particular case where $\xi = 0$ .

# 2.2. Modeling Threshold Excesses

# 2.2.1. Methods of Threshold Selection

Given a sequence of independent and identically distributed variables, $X _ { 1 } , \cdots , X _ { n }$ and a high threshold,$u$ , the excesses of these variables are $\left\{ x _ { i } : x _ { i } > u \right\}$ . Grouping these values as $x _ { ( 1 ) } \leq \dots \leq x _ { ( k ) }$ (all thevalues in the data set that are greater than the fixed value of the threshold $u$ ), the threshold excessesare defined by: $y _ { j } = x _ { ( j ) } - u$ for $j = 1 , \cdots , k$ . Applying Theorem 2.2, these excesses correspond toindependent observations of a random variable whose distribution function can be approximated by amember of the generalized Pareto family if the considered threshold is extreme enough.

One problem in order to use this approach is the choice of the threshold. Choosing a too highthreshold leads to small samples and the model would not be very accurate due to the lack of data. Ifthe chosen threshold was too low, a very little number of observations would be real extreme values.

Two different methods can be considered in order to select that threshold.

The first method consists on the study of the mean residual life plot, which focuses on the mean ofthe generalized Pareto distribution. If Y is a random variable with generalized Pareto distributionwith parameters $\sigma _ { u }$ and $\xi$ , it holds

$$
E (Y) = \left\{ \begin{array}{l l} \frac {\sigma_ {u}}{1 - \xi} & \xi <   1 \\ \infty & \xi \geq 1 \end{array} \right.
$$

This method is based on the behaviour of the distribution of the excesses when the value of thethreshold $u$ changes.

Let $\sigma _ { u _ { 0 } }$ be the scale parameter of the generalized Pareto distribution which corresponds to theexcess of the threshold $u _ { 0 }$ and let $\xi < 1$ . Given $Y _ { 1 } , \cdots , Y _ { n }$ a series of variables such that thegeneralized Pareto distribution is a valid distribution for the excesses over a threshold $u _ { 0 }$ andtaking Y an arbitrary element of the series,

$$
E (Y - u _ {0} \mid Y > u _ {0}) = \frac {\sigma_ {u _ {0}}}{1 - \xi}
$$

If the distribution is valid for $u _ { 0 }$ , it is valid for any $u > u _ { 0 }$ . Hence, for $u > u _ { 0 }$

$$
E (Y - u \mid Y > u) = \frac {\sigma_ {u}}{1 - \xi} = \frac {\sigma_ {u _ {0}} + \xi (u - u _ {0})}{1 - \xi}
$$

as $\begin{array} { r } { \sigma _ { u } = \sigma + \xi ( u - \mu ) } \end{array}$ . It is actually a linear function of $u$ . According to this, the function$E ( Y - u _ { 0 } \mid Y > u _ { 0 } )$ is expected to change linearly with $u$ for values of $u$ for which the Paretodistribution is appropriate.

Let $x _ { ( 1 ) } , \cdots , x _ { ( k ) }$ be the $n _ { u }$ observations that exceed a threshold $u$ , and $x _ { m a x }$ the largest of the $X _ { i }$ .The mean residual life plot is defined as the locus of points of the form

$$
\left\{\left(u, \frac {1}{n _ {u}} \sum_ {i = 1} ^ {n _ {u}} \left(x _ {(i)} - u\right): u <   x _ {\max }\right) \right\} \tag {2.8}
$$

Thus, under a right choice of threshold $u$ for which the generalized Pareto distribution approxim-ates to the distribution of excesses, $x _ { ( 1 ) } , \cdots , x _ { ( k ) }$ , the mean residual plot must be linear.

The second method is based on the estimation of the model using a sequence of values for thethreshold $u$ , taking into account the linear relation of $u$ and $\sigma _ { u }$ and knowing that $\xi$ should beconstant with respect to $u$ .

If the generalized Pareto distribution is valid for a threshold $u _ { 0 }$ , then according to Theorem 2.2 ex-cesses for a higher threshold also follow a Pareto distribution. Let $\sigma _ { u }$ be the value of the scale para-meter of a Pareto distribution for a threshold $u > u _ { 0 }$ . Then following (2.3), $\begin{array} { r } { \sigma _ { u } = \sigma _ { u _ { 0 } } + \xi \left( u - u _ { 0 } \right) } \end{array}$ ,so the scale parameter changes if $\xi \neq 0$ . This inconvenience is avoided by reparameterizing

$$
\boldsymbol {\sigma} ^ {*} = \boldsymbol {\sigma} _ {u} - \boldsymbol {\xi} \boldsymbol {u} = \boldsymbol {\sigma} _ {u _ {0}} - \boldsymbol {\xi} \boldsymbol {u} _ {0}
$$

and therefore $\sigma ^ { * }$ and $\xi$ should be constant for any $u > u _ { 0 }$ above $u _ { 0 }$ if $u _ { 0 }$ is a valid threshold.

The estimations of these parameters will not be exactly constant due to sampling variability, butapproximately if $u _ { 0 }$ is a valid threshold. Therefore, confidence intervals for both quantities, $\sigma ^ { * }$and $\xi$ can be plotted against $u$ . As they both have to be constant as $u$ changes, $u _ { 0 }$ should be thelowest value of $u$ for which the estimates remain near-constant.

The confidence interval for $\hat { \xi }$ can be obtained from the variance-covariance matrix V (see Sec-tion 2.2.2) and confidence intervals for $\hat { \sigma } ^ { * }$ con be obtained using the delta method,

$$
V a r \left(\boldsymbol {\sigma} ^ {*}\right) \approx \nabla \boldsymbol {\sigma} ^ {* T} V \nabla \boldsymbol {\sigma} ^ {*}
$$

where

$$
\nabla \sigma^ {* T} = \left[ \frac {\partial \sigma^ {*}}{\partial \sigma_ {u}}, \frac {\sigma^ {*}}{\partial \xi} \right] = [ 1, - u ]
$$

# 2.2.2. Parameter and return level estimation

# Method of Maximum likelihood

Once a valid threshold is determined, a useful method to estimate the parameters of the generalizedPareto distribution is maximum likelihood.

Let $x _ { ( 1 ) } , \cdots , x _ { ( k ) }$ be the $k$ excesses of a valid threshold $u$ . For $\xi \neq 0$ , using (2.3), if $H$ is the distribu-tion function, the likelihood function is

$$
L (\sigma , \xi) = \prod_ {i = 1} ^ {k} h (x _ {i}) = \prod_ {i = 1} ^ {k} H ^ {\prime} (x _ {i}) = \prod_ {i = 1} ^ {k} \frac {1}{\xi} \left(1 + \frac {\xi x _ {i}}{\sigma}\right) ^ {- 1 - 1 / \xi} \frac {\xi}{\sigma} = \prod_ {i = 1} ^ {k} \sigma^ {- 1} \left(1 + \frac {\xi x _ {i}}{\sigma}\right) ^ {- 1 - 1 / \xi}
$$

Hence, taking logarithms,

$$
l (\sigma , \xi) = \sum_ {i = 1} ^ {k} \log \sigma^ {- 1} + \sum_ {i = 1} ^ {k} \log \left(1 + \frac {\xi x _ {i}}{\sigma}\right) ^ {- 1 - 1 / \xi} = - k \log \sigma - \left(1 + \frac {1}{\xi}\right) \sum_ {i = 1} ^ {k} \log \left(1 + \frac {\xi x _ {i}}{\sigma}\right) \tag {2.9}
$$

with $1 + \frac { \xi x _ { i } } { \sigma } > 0$ for $i = 1 , \cdots , k$ . If not, $l ( \sigma , \xi ) = - \infty$ .

Similarly, using (2.7), the log-likelihood function for $\sigma$ for $\xi = 0$ is

$$
l (\sigma) = - k \log \sigma - \sigma^ {- 1} \sum_ {i = 1} ^ {k} x _ {i} \tag {2.10}
$$

It is not possible to make an analytical maximization of the log-likelihood in (2.9). Numerical methodsare needed, ensuring that the techniques are evaluated under the valid parameter space and payingspecial attention in order to avoid instabilities for $\xi \approx 0$ .

# Return levels

Suppose that a generalized Pareto distribution, $X$ , with parameters $\sigma$ and $\xi$ is valid for a thresholdu. That is, assuming $\xi > 0$ and $x > u$ ,

$$
\wp \{X > x \mid X > u \} = \left[ 1 + \xi \left(\frac {x - u}{\sigma}\right) \right] ^ {- 1 / \xi}
$$

Calling $\wp \{ X > u \} = \zeta _ { u }$ , by definition of conditional probability,

$$
\wp \{X > x \} = \zeta_ {u} \left[ 1 + \xi \left(\frac {x - u}{\sigma}\right) \right] ^ {- 1 / \xi}
$$

Hence, using the same idea of return level given in Section 1.1.3, the level $x _ { m }$ that exceeds on averageonce every m time periods (usually years) is the solution of

$$
\zeta_ {u} \left[ 1 + \xi \left(\frac {x _ {m} - u}{\sigma}\right) \right] ^ {- 1 / \xi} = \frac {1}{m}
$$

and solving this equation, for a sufficiently large m,

$$
x _ {m} = u + \frac {\sigma}{\xi} \left[ \left(m \zeta_ {u}\right) ^ {\xi} - 1 \right] \tag {2.11}
$$

Equivalently, if $\xi = 0$ and using (2.7) for m sufficiently large, it leads to

$$
x _ {m} = u + \sigma \log \left(m \zeta_ {u}\right) \tag {2.12}
$$

Note 2.6. From (2.11) and (2.12), the plot of $x _ { m }$ against m on a logarithmic scale leads to the samecases obtained in Note 1.10: $i f \xi = 0$ linearity, if $\xi > 0$ convexity and if $\xi < 0$ concavity.

It is common to give return levels on an annual scale. If $n _ { y }$ is the number of observations peryear and the N-year return level is the level expected once every $N$ years then $m = N \times n _ { y }$ . Hence,using (2.11), the N-year return level is defined by

$$
z _ {N} = u + \frac {\sigma}{\xi} \left[ \left(N n _ {y} \zeta_ {u}\right) ^ {\xi} - 1 \right]
$$

The estimation of return levels requires the previous estimation of $\zeta _ { u } , \sigma , \xi$ . Maximum likelihoodestimations of the parameters obtained in (2.9) and (2.10) can be used as estimations of $\xi$ and $\sigma$ .

A natural estimator of $\zeta _ { u }$ ,

$$
\hat {\zeta} _ {u} = \frac {k}{n},
$$

which represents the sample proportion of points that exceed the threshold $u$ , can be used in order toobtain an estimation. The number of excesses of $u$ has a binomial distribution $B i n ( n , \zeta _ { u } )$ so the naturalestimator is the maximum likelihood estimator. The variance of $\hat { \zeta } _ { u }$ can be obtained from the propertiesof the binomial distribution as follows,

$$
V a r \left(\hat {\zeta} _ {u}\right) \approx \hat {\zeta} _ {u} \left(1 - \hat {\zeta} _ {u}\right) / n
$$

Therefore, the variance-covariance matrix of $( \hat { \zeta } _ { u } , \hat { \sigma } , \hat { \xi } )$ is approximately

$$
V = \left[ \begin{array}{c c c} \hat {\zeta} _ {u} \left(1 - \hat {\zeta} _ {u}\right) / n & 0 & 0 \\ 0 & v _ {1, 1} & v _ {1, 2} \\ 0 & v _ {2, 1} & v _ {2, 2} \end{array} \right] \tag {2.13}
$$

where $\hat { \sigma }$ and $\hat { \xi }$ are the maximum likelihood estimations of $\sigma$ and $\xi$ and $\nu _ { i , j }$ denotes the $( i , j )$ th termof the variance-covariance matrix of $\hat { \sigma }$ and $\hat { \xi }$ . By the multivariate delta method, $V a r ( \boldsymbol { x } _ { m } ) \approx \nabla { x _ { m } } ^ { T } V \nabla { x _ { m } }$ ,where

$$
\begin{array}{l} \nabla x _ {m} ^ {T} = \left[ \frac {\partial x _ {m}}{\partial \zeta_ {u}}, \frac {\partial x _ {m}}{\partial \sigma}, \frac {\partial x _ {m}}{\partial \xi} \right] = \\ = \left[ \sigma m ^ {\xi} \zeta_ {u} ^ {\xi - 1}, \xi^ {- 1} \left\{\left(m \zeta_ {u}\right) ^ {\xi} - 1 \right\}, - \sigma \xi^ {- 2} \left\{\left(m \zeta_ {u}\right) ^ {\xi} - 1 \right\} + \sigma \xi^ {- 1} \left(m \zeta_ {u}\right) ^ {\xi} \log \left(m \zeta_ {u}\right) \right], \\ \end{array}
$$

evaluated at $( \hat { \zeta } _ { u } , \hat { \sigma } , \hat { \xi } )$ .

# 2.2.3. Model checking

In order to verify the GPD behaviour of the sample, plotting quantiles and probabilities againstdensity and return level functions can be very useful.

Let $u$ be a threshold and let $x _ { ( 1 ) } , \ldots , x _ { ( k ) }$ be threshold excesses as obtained in Section 2.2.1. Thus,$x _ { ( 1 ) } \leq \dots \leq x _ { ( k ) }$ . The probability plot consists of the points

$$
\left\{\left(\frac {i}{k + 1}, H (x _ {(i)})\right); i = 1, \dots , k \right\}
$$

where H is the Pareto distribution function with parameters $( \hat { \sigma } , \hat { \xi } )$ given by (2.3). If $\xi = 0$ then thePareto distribution obtained in (2.7) should be considered.

Now, let

$$
H ^ {- 1} (y) = u + \frac {\hat {\sigma}}{\hat {\xi}} \left[ y ^ {- \hat {\xi}} - 1 \right]
$$

The quantile plot is given by

$$
\left\{\left(H ^ {- 1} \left(\frac {i}{k + 1}\right), x _ {(i)}\right); i = 1, \dots , k \right\}
$$

According to [3], if the GPD is a good model for the excesses over the threshold $u$ , then both plotsshould be linear.

# Chapter 3

# Point processes

The method of excesses over threshold (EOT) is based on the fact that excesses over a fixed value uhave a generalized Pareto distribution and the occurrence of these excesses is a Poisson process.

This chapter will show that the point process of extreme values is closely related to both the GEVfamily of distributions and the GPD. Methods for the study of extremes on dependent sets of data arealso given in this chapter in order to apply these results in a practical case.

# 3.1. Point processes of exceedances

# 3.1.1. Poisson process and some definitions

An intuitive way to describe a point process $N$ is defining it as a random distribution of points $X _ { i }$ inspace. For a group of points $( X _ { i } )$ and a set $A \in \mathbb { R }$ , $N ( A )$ can be described as a measure that counts thenumber of points $X _ { i }$ in $A$ .

Definition 3.1. A Poisson process in $\mathbb { R } ^ { + }$ with parameter $\lambda ( t )$ is a point process which verifies that forany t ,

$$
\wp (N (t, t + \delta) = 1 | H _ {t}) = \lambda (t) + o (\delta)
$$

$$
\wp (N (t, t + \delta) > 1 | H _ {t}) = o (\delta)
$$

where $H _ { t }$ is the process behaviour until time t and $N ( t _ { 1 } , t _ { 2 } )$ is the number of points in $\left( t _ { 1 } , t _ { 2 } \right]$ . If $\lambda ( t )$ is aconstant, the process is said to be homogeneous (HPP), and non-homogeneous (NHPP) otherwise.

The verification of a Poisson process might not be intuitive but using the previous definition thereare some characterizations of the process that can be easier to check [1, 12].

Let $N ( A )$ be the random variable that denotes the number of points that occur at an arbitraryperiod of time A. In a HPP with disjoint sets $A _ { 1 } , A _ { 2 } , \ldots$ , the random variables $N ( A _ { 1 } ) , N ( A _ { 2 } ) , \ldots$are independent and have Poisson distribution $A _ { i } \sim P o i s s o n ( \lambda | A _ { i } | )$ where $\left| A _ { i } \right|$ is the length of $A _ { i }$for $i = 1 , 2 , \dots$ .

Arrival times at a point process are defined as time from one to another consecutive event, that is,$T _ { r 1 } = T _ { 1 } , T _ { r 2 } = T _ { 2 } - T _ { 1 } , \ldots .$ In the case of a HPP, arrival times are independent random variableswith distribution $E x p ( \lambda )$ .

Given any period of time $A$ , the intensity measure of the process is defined as

$$
\Lambda (A) = E \left\{N (A) \right\} = \int_ {A} \lambda (t) d t
$$

which gives the expected number of points in the set $A$ (see [2]). The intensity function $\lambda ( t )$ is deducedfrom this definition as the derivative of $\Lambda ( A )$ , that is, $\lambda ( t ) = \Lambda ^ { \prime } ( t )$

# 3.1.2. Convergence law of point processes

Let $X _ { 1 } , X _ { 2 } , \dots$ be independent and identically distributed random variables which satisfy Theorem 1.4and let a bidimensional scaling point process be defined as follows

$$
N _ {n} = \left\{\left(\frac {i}{n + 1}, \frac {X _ {i} - b _ {n}}{a _ {n}}\right), i = 1, \dots , n \right\} \tag {3.1}
$$

where $a _ { n }$ and $b _ { n }$ normalize the behaviour of random variables.

Let $A = [ 0 , 1 ] \times ( u , \infty )$ be a region of $\mathbb { R } ^ { 2 }$ for some value $u$ . The probability of each point of $N _ { n }$dropping in $A$ is given by

$$
p = \wp \left\{\left(X _ {i} - b _ {n}\right) / a _ {n} > u \right\} \approx \frac {1}{n} \left[ 1 + \xi \left(\frac {u - \mu}{\sigma}\right) \right] ^ {- 1 / \xi}
$$

As $X _ { i }$ are mutually independent, the distribution $N _ { n } ( A )$ is binomial with $N _ { n } ( A ) \approx \mathrm { B i n } ( n , p )$ .

For a large enough $n$ , ${ \bf B i n } ( n , p ) \approx { \bf P o i } ( n p )$ . Thus, $N _ { n } ( A ) \to N ( A )$ where $N ( A ) \sim \operatorname { P o i } ( \Lambda ( A ) )$ and

$$
\Lambda (A) = \left[ 1 + \xi \left(\frac {u - \mu}{\sigma}\right) \right] ^ {- 1 / \xi}
$$

This result is formalized in the following theorem.

Theorem 3.2. Let $X _ { 1 } , X _ { 2 } , \dots$ . be independent and identically distributed random variables such thatthere exist sequences of constants $\{ a _ { n } > 0 \}$ and $\left\{ b _ { n } \right\}$ that satisfy

$$
\lim  _ {n \rightarrow \infty} \wp \left\{\left(M _ {n} - b _ {n}\right) / a _ {n} \leq z \right\} = G (z)
$$

where $G ( z )$ is a member of the GEV family of distributions, and let z− and $z _ { + }$ be the lower and upperend points of $G$ and $N _ { n }$ a bidimensional scaling point process defined as in (3.1). Then $N _ { n }$ converges onregions in $( 0 , 1 ) \times [ u , \infty )$ for any $u > z _ { - }$ to a Poisson process N with intensity measure on an arbitraryregion $A = \left[ t _ { 1 } , t _ { 2 } \right] \times \left[ z , z _ { + } \right)$ given by

$$
\Lambda (z) = \left(t _ {2} - t _ {1}\right) \left[ 1 + \xi \left(\frac {z - \mu}{\sigma}\right) \right] ^ {- 1 / \xi} \tag {3.2}
$$

From the theorem (see [3, Theorem 7.1]), it is easy to see that the intensity function of this processis given by

$$
\lambda (z) = \left[ 1 + \xi \left(\frac {z - \mu}{\sigma}\right) \right] ^ {- 1 / \xi}
$$

# 3.2. Relation with other extreme value models

This section shows how the GEV distribution for maxima and the Pareto distribution can also beobtained in terms of Poisson processes.

# 3.2.1. Relation with the GEV family of distributions

Let $M _ { n }$ be the maximum of $X _ { 1 } , \ldots , X _ { n }$ as usual and

$$
N _ {n} = \left\{\left(\frac {i}{n + 1}, \frac {X _ {i} - b _ {n}}{a _ {n}}\right) f o r i = 1, \ldots , n \right\}
$$

Taking $A _ { z } = \{ ( 0 , 1 ) \times ( z , \infty ) \}$ , the following events verify

$$
\left\{\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z \right\} \approx \left\{N _ {n} \left(A _ {z}\right) = 0 \right\}
$$

and therefore,

$$
\wp \left\{\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z \right\} = \wp \left\{N _ {n} \left(A _ {z}\right) = 0 \right\}\rightarrow \wp \left\{N \left(A _ {z}\right) = 0 \right\}
$$

and since $N$ follows a Poisson distribution,

$$
\wp \left\{N \left(A _ {z}\right) = 0 \right\} = \exp \left\{- \Lambda \left(A _ {z}\right) \right\} = \exp \left[ - \left(1 + \xi \frac {z - \mu}{\sigma}\right) ^ {- 1 / \xi} \right]
$$

so the limit of the normalized distribution of maxima is the GEV family of distributions.

# 3.2.2. Relation with generalized Pareto family of distributions

Let $\Lambda ( A _ { z } ) = \Lambda _ { 1 } \left( \left[ t _ { 1 } , t _ { 2 } \right] \right) \times \Lambda _ { 2 } \left( \left[ z , \infty \right) \right)$ be a factorization of the intensity measure defined in (3.2),$\Lambda ( A _ { z } )$ , such that

$$
\Lambda_ {1} \left(\left[ t _ {1}, t _ {2} \right]\right) = \left(t _ {2} - t _ {1}\right) a n d \Lambda_ {2} \left(\left[ z, \infty\right)\right) = \left(1 + \xi \frac {u - \mu}{\sigma}\right) ^ {- 1 / \xi}
$$

Then,

$$
\begin{array}{l} \wp \left\{\left(X _ {i} - b _ {n}\right) / a _ {n} > z \mid \left(X _ {i} - b _ {n}\right) / a _ {n} > u \right\} = \frac {\Lambda_ {2} ([ z , \infty))}{\Lambda_ {2} ([ u , \infty))} = \\ = \left[ \frac {1 + \xi (z - \mu) / \sigma}{1 + \xi (u - \mu) / \sigma} \right] ^ {- 1 / \xi} = \left[ 1 + \frac {\xi (z - u) / \sigma}{1 + \xi (u - \mu) / \sigma} \right] ^ {- 1 / \xi} = \left[ 1 + \xi \left(\frac {z - u}{\sigma_ {u}}\right) \right] ^ {- 1 / \xi} \\ \end{array}
$$

where $\boldsymbol { \sigma _ { u } } = \boldsymbol { \sigma } + \boldsymbol { \xi } \left( \boldsymbol { u } - \boldsymbol { \mu } \right)$ , which leads to the generalized Pareto distribution.

# 3.3. Estimations of the Poisson process

# 3.3.1. Estimation of parameters

Data modelling needs the estimation of the process given a set of observed points $x _ { 1 } , \ldots , x _ { n } \in { \mathcal { A } }$ .Let a non-homogeneous one-dimensional processes be considered and let the intensity function be ofthe form $\lambda ( \cdot , \theta )$ , where $\theta$ is an unknown vector. Thus, the only parameters which have to be estimatedare the componente of the vector θ . Likelihood estimation can be used writing probabilities as functionsof $\theta$ (see [6]).

Let $I _ { i } = [ x _ { i } , x _ { i } + \delta _ { i } ]$ for $i = 1 , \ldots , n$ be small intervals that represent the observations’ neighbourhoodsand ${ \mathcal { I } } = { \mathcal { A } } \backslash \cup _ { i = 1 } ^ { n } I _ { i }$ . Using the Poisson process definition,

$$
\wp \left\{N \left(I _ {i}\right) = 1 \right\} = e x p \left\{- \Lambda \left(I _ {i}; \theta\right) \right\} \Lambda \left(I _ {i}; \theta\right)
$$

where

$$
\Lambda (I _ {i}; \theta) = \int_ {x _ {i}} ^ {x _ {i} + \delta_ {i}} \lambda (u) d u \approx \lambda (x _ {i}) \delta_ {i}
$$

and substituting,

$$
\wp \left\{N \left(I _ {i}\right) = 1 \right\} \approx e x p \left\{- \lambda \left(x _ {i}\right) \delta_ {i} \right\} \lambda \left(x _ {i}\right) \delta_ {i} \approx \lambda \left(x _ {i}\right) \delta_ {i}
$$

for small $\delta _ { i }$ . Therefore the following equality holds,

$$
\begin{array}{l} L (\theta ; x _ {1}, \dots , x _ {n}) = \wp \left\{N (\mathcal {I} = 0), N (I _ {1}) = 1, \dots , N (I _ {n}) = 1 \right\} = \\ = \wp \left\{N (\mathcal {I} = 0) \right\} \prod_ {i = 1} ^ {n} \wp \left\{N \left(I _ {i}\right) = 1 \right\} \approx \exp \left\{- \Lambda (\mathcal {A}; \boldsymbol {\theta}) \right\} \prod_ {i = 1} ^ {n} \lambda \left(x _ {i}\right) \delta_ {i} \\ \end{array}
$$

Maximization of this likelihood often requires numerical methods.

Estimations of parameters $( \mu , \sigma , \xi )$ can be obtained from the GEV and Pareto estimation methodsas the connection between parameters from Poisson processes and these distributions has been observed.

# 3.3.2. Estimation of return levels

Estimations of return levels in stationary processes are easy to obtain. For non-stationary sequences,estimations can be obtained as well but they depend on the model.

Let $z _ { m }$ be the m-year return level as usual. In the case of the stationary point process, the followingresult holds

$$
1 - \frac {1}{m} = \wp \{m a x (X _ {1}, \dots , X _ {n}) \leq z _ {m} \} \approx \prod_ {i = 1} ^ {n} p _ {i} = \prod_ {i = 1} ^ {n} p = p ^ {n}
$$

where

$$
p = \left\{ \begin{array}{l l} 1 - n ^ {- 1} \left[ 1 + \xi \left(z _ {m} - \mu\right) / \sigma \right] ^ {- 1 / \xi} & \text {i f} \left[ 1 + \xi \left(z _ {m} - \mu\right) / \sigma \right] > 0 \\ 1 & \text {o t h e r w i s e} \end{array} \right.
$$

and $( \mu , \sigma , \xi )$ are the parameters of the Poisson process. Making calculations and substituting,

$$
1 - \frac {1}{m} = \left[ 1 - \frac {1}{n} \left[ 1 + \xi \frac {z _ {m} - \mu}{\sigma} \right] ^ {- 1 / \xi} \right] ^ {n} \Rightarrow n - n \left(1 - \frac {1}{m}\right) ^ {1 / n} = \left[ 1 + \xi \frac {z _ {m} - \mu}{\sigma} \right] ^ {- 1 / \xi}
$$

Therefore,

$$
z _ {m} = \mu + \frac {\sigma}{\xi} \left[ \left[ n - n \left(1 - \frac {1}{m}\right) ^ {1 / n} \right] ^ {- \xi} - 1 \right]
$$

# 3.4. Extremes on dependent sequences

Every extreme value model that has been described was obtained supposing that events were in-dependent. Nevertheless, this assumption is not usually true. In this section, some results will beconsidered in order to describe dependence conditions for which extreme value theorems are still valid.

Definition 3.3. A series $X _ { 1 } , X _ { 2 } , \dots$ is said to be stationary if for any finite set $t _ { 1 } < \ldots < t _ { n }$ and $h \in \mathbb { Z }$ ,

$$
\left(X _ {t _ {1}}, \dots , X _ {t _ {n}}\right) \stackrel {{d}} {{=}} \left(X _ {t _ {1} + h}, \dots , X _ {t _ {n} + h}\right)
$$

Definition 3.4. A stationary series satisfies the $D ( u _ { n } )$ condition $i f \forall i _ { 1 } , \ldots , i _ { k } , j _ { 1 } , \ldots , j _ { l }$ with $j _ { 1 } - i _ { k } > h$we have

$$
\begin{array}{l} \left| \wp \left\{X _ {i _ {1}} \leq u _ {n}, \dots , X _ {i _ {k}} \leq u _ {n}, X _ {j _ {1}} \leq u _ {n}, \dots , X _ {j _ {l}} \leq u _ {n} \right\} \right. \\ - \wp \left\{X _ {i _ {1}} \leq u _ {n}, \dots , X _ {i _ {k}} \leq u _ {n} \right\} \wp \left\{X _ {j _ {1}} \leq u _ {n}, \dots , X _ {j _ {l}} \leq u _ {n} \right\} | \leq \alpha (n, h) \tag {3.3} \\ \end{array}
$$

where $\alpha ( n , h _ { n } )  0$ for sequence $\left( h _ { n } \right)$ such that $\scriptstyle \operatorname* { l i m } _ { n \to \infty } h _ { n } / n = 0$ .

This condition is weaker than the independence of events and it is verified in cases where observa-tions are mostly independent when there is a sufficiently large temporal distance between them. Usingthis, the following theorem holds for a stationary sequence.

Theorem 3.5. Let $X _ { 1 } , X _ { 2 } , \dots$ be a stationary sequence and define $M _ { n } = m a x \{ X _ { 1 } , \ldots , X _ { n } \}$ . Then if the$D ( u _ { n } )$ condition is satisfied for $u _ { n } = a _ { n } z + b _ { n }$ , $\forall z \in \mathbb { R }$ , for sequences of constants $\{ a _ { n } > 0 \}$ and $\left\{ b _ { n } \right\}$such that

$$
\lim  _ {n \rightarrow \infty} \wp \left\{\frac {M _ {n} - b _ {n}}{a _ {n}} \leq z \right\} = G (z)
$$

where $G$ is a non-degenerate distribution function, then $G$ is a member of the GEV family of distribu-tions.

Therefore, the $D ( u _ { n } )$ condition can be understood as a property that guarantees the weakness of thedependence of data so that it does not affect the distribution of maxima.

The main assumption in order to fit a Pareto distribution is that threshold excesses series must beindependent (see Theorem 2.2). In the case of stationary sequences, excesses can only be treated asif they were independent events if Definition 3.4 is satisfied. However, the behaviour of the extremedistribution of neighbouring excesses has not yet been considered.

When the variables are not independent, threshold excesses are expected to appear in clusters, whichimplies that one excess can be easily followed by another. Thus, the log-likelihood method cannot beapplied due to the dependence of observations.

In order to find a set of approximately independent excesses, the most common method is declus-tering (see [3]), which consists on a filtering of the dependent excesses in order to obtain a set ofexceedances that are approximately independent.

The first step is defining clusters of excesses using an empirical rule. The maximum excess in eachcluster is taken and assuming cluster maxima to be independent, a generalized Pareto distribution canbe fitted to these values.

# Chapter 4

# Practical example: extreme distributionsof water levels in the Ebro river inZaragoza

This chapter will show a practical example of how to fit extreme value distributions to a data set.The data that has been used are daily mean water levels measured in metres from 1961 to 2016 in theEbro river in Zaragoza (Spain). The data series can be found on the Sistema Automático de InformaciónHidrológica (SAIH) official webpage [4]. The date of each observation is also available in the SAIHfile.

The behaviour of water level over time will be studied in order to fit an appropriate distribution tothe observations. A GEV distribution and a Pareto distribution can be fitted to this set of data and ananalysis of return levels can be easily made once the parameters of the distributions are estimated.

The data will be analysed using the system for statistical computation and graphics, R. The R lan-guage (see [15]) is worldwide used by mathematicians for the development of statistical software anddata analysis. The analysis of extremes is distributed in many packages with a lot of different applica-tions (see [5]). The packages which will be used in this analysis are the Rcmdr, the extRemes and theismev ([7, 8, 10]). The complete R code used in this chapter can be found in Appendix A.

# 4.1. Data and exploratory analysis

The application of the extreme value models requires the data to be independent and identicallydistributed. However, it has been seen in Section 3.4 that weaker conditions can be considered. Ifthe series is stationary and it does not show long term dependence, the extreme value theorems arevalid . Thus, the first hypothesis which has to be analysed is the stationarity of the data series and thedependence of water level values.

Because of their physical characteristics, the observations of the height of the water in the river areclearly dependent but they do not show a long time dependence, that is, the water level one day is relatedto the level the following day but it is independent to the height of the water six months later. The twomain reasons for which there can be changes in parameters of the distributions are the existence of trendand stationarity behaviour (see [11]).

Figure 4.1 (left) represents a scatter plot of water level observations where each point is the meanlevel of the water in metres per month and year. With this plot it is easy to see that the points representingthe 1960s are shifted and the minimum values in those years are lower than in the rest of the decades.Thus, events are not identically distributed and this can affect the estimations of parameters. Thereforethe analysis shall be focused on data from 1970 to 2016.

In order to study the existence of a seasonal pattern of the series, a box plot representing the vari-ability of data per month can be given in order to understand the behaviour at each time of the year. In

![](images/80ed54da350bb38f802a5a33f70121a7f381333fce16e01909c41510dc02a0a8.jpg)


![](images/2e708819b2734fbed80e789c9a5bc8004a783ab45b624567fd6b146a0de3ac04.jpg)



Figure 4.1: Scatter plot of data from 1961 to 2016 (left) and box plot of water levels per month of theyear (right)


Figure 4.1 (right), months with approximately identically distribution can be checked.

The plot suggests that water levels present seasonal behaviour along the year. December and Januarythrough April seem to be approximately identically distributed. Thus, these months are suitable for theanalysis. From this set of data, the extreme distributions will correspond in fact to the study of annualmaximums.

# 4.2. Extreme behaviour and extreme distributions of data

# 4.2.1. GEV approximation

From the data in December and January through April, the set of annual maxima are represented ina scatter plot. It is easy to see that in Figure 4.2, the points appear as a point cloud with no pattern. Thisconfirms the hypothesis of identical distribution of the annual maximum set of points and therefore ageneralized extreme value distribution can be fitted to this set.

![](images/9ca568c87c371981b0d3728e74b1644a5e47e703daec88b1eede1229df2049f4.jpg)



Figure 4.2: Scatter plot of maximum water level per year


From the set of data given in Figure 4.2, a distribution of the following form can be obtained usingfevd (extRemes R Package [8]),

$$
G (z) = \exp \left(- \left(1 + \xi \left(\frac {z - \mu}{\sigma}\right)\right) ^ {- 1 / \xi}\right)
$$

where the location parameter $\hat { \mu } = 3 . 4 8$ , the scale parameter $\hat { \sigma } = 0 . 8 5$ and the shape parameter $\hat { \xi } =$$- 0 . 2 3$ . As $\hat { \xi } < 0$ , the GEV distribution corresponds to a Weibull distribution according to Note 1.6.

Once the GEV distribution is estimated, return levels can easily be obtained. The direct applicationof Section 1.1.3, the choice of a range of values and the function return.level (extRemes R Package)gives,

![](images/b07eab584f5a3d678e38956e2b25bce28dff36250d2444c189847bda487c855a.jpg)



Figure 4.3: Return levels and their $9 5 \%$ confidence intervals for the GEV distribution


<table><tr><td>1/p</td><td>5</td><td>10</td><td>25</td><td>50</td><td>100</td><td>200</td></tr><tr><td>95% lower CI</td><td>4.24</td><td>4.61</td><td>4.93</td><td>5.07</td><td>5.14</td><td>5.18</td></tr><tr><td>zp</td><td>4.55</td><td>4.97</td><td>5.39</td><td>5.66</td><td>5.88</td><td>6.06</td></tr><tr><td>95% upper CI</td><td>4.87</td><td>5.31</td><td>5.86</td><td>6.24</td><td>6.60</td><td>6.95</td></tr></table>

According to Figure 4.3, the maximum is expected to be greater than 4.5 metres once every 5 yearsand the probability of getting a level of 4.55 metres is $1 / 5 = 0 . 2$ . In a similar way, taking $1 / p = 2 5$ , themaximum exceeds 5.4 metres once every 25 years and the river has a probability of reaching the height5.4 metres of $p = 0 . 4$ .

# 4.2.2. GPD approximation

The first step to fit a generalized Pareto distribution is the selection of the threshold. It is an importantstep since if the threshold is too high there might be a lack of data while if it is too low the GPDapproximation will not be valid as there might be some excesses which are not real extreme values.

Figure 4.4 shows the mean residual life plot with $9 5 \%$ of confidence interval of the high level ofwater obtained using mrl.plot (ismev R Package [10]). It plots a threshold $u$ against the mean of theexceedances of the threshold, for a range of thresholds.

The plot is linear from $u \approx 3$ until $u \approx 4$ . Applying Section 2.2.1, if the threshold $u = 3$ is valid, anythreshold $u > 3$ is valid as well. Thus, the mean residual life plot should be linear after $u = 3$ . Due toa lack of exceedances over thresholds $u > 4$ , the estimation of the mean residual life plot is unreliablealthough it is in the confidence interval. Because of this, the mean residual life plot is not linear after$u = 4$ .

Thus, $u = 3$ is considered. As data presents a short time dependence, the estimation requires theuse of the declustering method for EOT explained in Section 3.4. The function decluster (extRemesR Package [8]) applies a declustering method to the threshold excesses which consists in providing themaximum water level of each cluster of exceedances, where each cluster is a set of months given bywater levels from December of a year and from January to April of the following year.

![](images/00f1c38e01179e2212e4430adce769646322b5a40aa4a0dc899f74bd71c46790.jpg)



Figure 4.4: Mean residual life plot of data with $9 5 \%$ of confidence interval


The number of excesses over the threshold $u = 3$ is 384, but the number of events taken into accountin order to fit a GPD is equal to 104. Then, the GPD obtained by using fevd (extRemes R Package [8])is given by

$$
H (x) = 1 - \left(1 + \frac {\hat {\xi} x}{\hat {\sigma}}\right) ^ {- 1 / \hat {\xi}}
$$

where the scale parameter $\hat { \sigma } = 0 . 7 9$ and the shape parameter $\hat { \xi } = - 0 . 1 4$ . Applying Note 2.3, as $\hat { \xi } < 0$ ,the distribution has an upper bound at $u - \hat { \sigma } / \hat { \xi } = \hat { 8 } . 6 4$ metres and the theoretical mean of excesses is$\bar { x } = \hat { \sigma } / ( 1 - \hat { \xi } ) = 0 . 6 9$ metres. Return levels of the excesses are obtained applying Section 2.2.2. For agiven range of values and using return.level (extRemes R Package),

![](images/b76f1bb1ff6550f715ba3344d91e553ab60eb976dcf0a590db2398850a88096d.jpg)



Figure 4.5: Return levels and their $9 5 \%$ confidence intervals for the GP distribution


<table><tr><td>1/p</td><td>5</td><td>10</td><td>25</td><td>50</td><td>100</td><td>200</td></tr><tr><td>95% lower CI</td><td>4.62</td><td>4.81</td><td>4.96</td><td>5.01</td><td>5.03</td><td>4.99</td></tr><tr><td>zp</td><td>5.07</td><td>5.40</td><td>5.79</td><td>6.05</td><td>6.29</td><td>6.50</td></tr><tr><td>95% upper CI</td><td>5.53</td><td>5.99</td><td>6.62</td><td>7.10</td><td>7.57</td><td>8.03</td></tr></table>

Looking at Figure 4.5 and the table, the probability of the occurrence of one particular water level is theinverse of the return period and the return level is expected to be exceeded once every $1 / p$ years.

The values of the estimated return levels are slightly different as they are calculated using differentsets of data, but the values of $z _ { p }$ obtained in each of the methods are in the confidence intervals of theother method respectively, so they are assumed to be good estimations.

# Appendix A

# R Code

# A.1. Data and exploratory analysis Code

```r
#Month and year variable (Time)  
> Datos$TIEMPO <- with(Datos, ANIO+((MES.NUMERO-1)/12))  
# Create data and mean per month variable  
> Mediames <- as.data.frame(tapply(Datos$TIEMPO, list(Datos$TIEMPO), min, na.rm=TRUE))  
> Mediames$Mean <- tapply(Datos$ALTURA, list(Datos$TIEMPO), mean, na.rm=TRUE)  
#Change of name "TIEMPO"  
> names(Mediames)[1] <- "Time"  
#Scatter plot of mean water level per month and year  
> scatterplot(Mean~Time, reg.line=FALSE, smooth=TRUE, spread=FALSE, boxplots=FALSE, span=0.3, ellipse=FALSE, levels=c(.5, .9), data=Mediames)  
#Calculate subset of data as the years required are from 1970 to 2016  
> Datos70 <- subset(Datos, subset= ANIO>1969)  
> Datos70 <- within(Datos70, {mesnumberofactor <- as.factor(MES.NUMERO)})  
#Change of name of variable "MES"  
> names(Datos70)[7] <- "Month"  
#Change of name "ALTURA"  
> names(Datos)[5] <- "Height"  
#Boxplot of Height of water per month of the year  
> Boxplot(Height~Month, data=Datos70, id.method="none")
```

# A.2. GEV approximation Code

```r
#Subset of data of months January to April and December
> Datosfin<-subset(Datos70, subset=MES.NUMERO<5|MES.NUMERO==12)
#Calculate set of data of maximum water level per year
> MaxGEV <- as.data.frame(tapply(Datosfin$ANIO, list(Datosfin$ANIO), min, na.rm=TRUE))
> MaxGEV$Maximum <- tapply(Datosfin$Height, list(Datosfin$ ANIO), max, na.rm=TRUE)
> max<-as.vector(MaxGEV$Maximum)
#Change name of variable "ANIO"
> names(MaxGEV)[1]<- "Year"
#Scatter plot of the distribution of maxima
> scatterplot(Maximum~Year, reg.line=FALSE, smooth=TRUE, spread=FALSE,
boxplots=FALSE, span=0.3, ellipse=FALSE, levels=c(.5, .9), data=MaxGEV)
```

```txt
#Fit a GEV distribution to the set of maxima  
>fit1<-fevd(max)  
#Calculate return levels given certain return periods and 95% confidence intervals  
> vec<-c(3,5,7,10,13,15,18,21,23,25,30,35,40,45,50,60,70,80,90,100,150,200)  
> z<-return.level fit1, return.period=vec, alpha=0.05, do.ci=TRUE)  
#Plot of return levels and their 95% confidence intervals for GEV  
#Vector of lower 95% CI  
> return1<-c(z[1], z[2], z[3], z[4], z[5], z[6], z[7], z[8], z[9], z[10], z[11], z[12], z[13], z[14], z[15], z[16], z[17], z[18], z[19], z[20], z[21], z[22])  
#Vector of return levels  
> return2<-c(z[23], z[24], z[25], z[26], z[27], z[28], z[29], z[30], z[31], z[32], z[33], z[34], z[35], z[36], z[37], z[38], z[39], z[40], z[41], z[42], z[43], z[44])  
#Vector of upper 95% CI  
> return3<-c(z[45], z[46], z[47], z[48], z[49], z[50], z[51], z[52], z[53], z[54], z[55], z[56], z[57], z[58], z[59], z[60], z[61], z[62], z[63], z[64], z[65], z[66])  
> plot vec, return2, xlab="Return Period (Years)", ylab="Return Level")  
> lines( vec, return1, type="l", col="Red", lwd=3)  
> lines( vec, return3, type="l", col="Red", lwd=3)
```

# A.3. GPD approximation Code

Vector of height of water
> alt<-as.vector(Datosfin\\(Height)
#Mean residual life plot with  $95\%$  confidence interval
> mrl.plot(alt,conf=0.95)
#Vector of groups for declustering
> bis<-c(152,151,151,151)
> vect<-c(rep(1,120),rep(2,151),rep(3:46,rep(bis,11)),rep(47,152),rep(48,31))
#Decluster of excesses with threshold u=3
> altitude<-Datasfin\\)Height
> dec<-decluster(altitude,threshold=3,method="intervals",groups=vec)
#Fit a generalized Pareto distribution
> fin<-fevd(dec, threshold=3, type="GP",time.units="days",period.basis="year")

Calculate return levels given certain return periods and  $95\%$  confidence intervals  $\begin{array}{rl} & {\mathrm{>vec <   - c(3,5,7,10,13,15,18,21,23,25,30,35,40,45,50,60,70,80,90,100,150,200)}}\\ & {\mathrm{>z <   - return level(fin,return.period = vec,alpha = 0.05,do.ai = TRUE)}} \end{array}$

Plot of return levels and their  $95\%$  confidence intervals for GPD   
#Vector of lower  $95\%$  CI   
>return1<-c(z[1],z[2],z[3],z[4],z[5],z[6],z[7],z[8],z[9],z[10],z[11],z[12],z[13], z[14],z[15],z[16],z[17],z[18],z[19],z[20],z[21],z[22])   
#Vector of return levels   
>return2<-c(z[23],z[24],z[25],z[26],z[27],z[28],z[29],z[30],z[31],z[32],z[33],z[34] , z[35],z[36],z[37],z[38],z[39],z[40],z[41],z[42],z[43],z[44])   
#Vector of upper  $95\%$  CI   
>return3<-c(z[45],z[46],z[47],z[48],z[49],z[50],z[51],z[52],z[53],z[54],z[55],z[56] , z[57],z[58],z[59],z[60],z[61],z[62],z[63],z[64],z[65],z[66])   
> plot(VEC,return2,xlab  $\equiv$  "Return Period (Years)","ylab  $\equiv$  "Return Level")

```txt
> lines vec, return1, type="l", col="Red", lwd=3)  
> lines vec, return3, type="l", col="Red", lwd=3)
```

# Bibliography



[1] A.C. CEBRIÁN GUAJARDO Análisis, modelización y predicción de episodios de sequía, Departa-mento de métodos estadísticos, Universidad de Zaragoza, 1999.





[2] S.G. COLES & R.S.J. SPARKS, Extreme value methods for modelling historical series of largevolcanic magnitudes, Statistics in volcanology. Geol Soc, London, 2006





[3] S. COLES, J. BAWA, L. TRENNER & P DORAZIO, An introduction to statistical modeling ofextreme values, Springer, Vol 208, 2001.





[4] CONFEDERACIÓN HIDROGRÁFICA DEL EBRO, Datos de altura media diaria en metros para es-tación de aforo 9011, 1961 to 2016. Data available at http://www.saihebro.com/saihebro/index.php.





[5] C. DUTANG & K. JAUNATRE, CRAN Task View: Extreme Value Analysis, 2017 https://CRAN.R-project.org/view $\underline { { \boldsymbol { \cdot } } } =$ ExtremeValue.





[6] P. EMBRECHTS, C. KLÜPPELBERG & T. MIKOSCH, Modelling extremal events: for insuranceand finance, Springer Science & Business Media, Vol 33, 2013.





[7] J.FOX & M.BOUCHET-VALAT, Rcmdr: R Commander, R package version 2.3-2, 2017, http://socserv.socsci.mcmaster.ca/ifox/Misc/Rcmdr/





[8] E. GILLELAND, Package ’extRemes’, 2016, https://cran.r-project.org/web/packages/extRemes/extRemes.pdf.





[9] E.J. GUMBEL, Statistics of extremes, New York: Columbia University Press, 1958.





[10] J.E. HEFFERNAN, A.G. STEPHENSON & E. GILLELAND, Ismev: an introduction to statisticalmodeling of extreme values, R package version, Vol 1, 2012, https://cran.r-project.org/web/packages/ismev/ismev.pdf.





[11] R.J. HYNDMAN & G. ATHANASOPOULOS, Forecasting: principles and practice, O Texts, 2014.





[12] N. LASKIN, Communications in Nonlinear Science and Numerical Simulation, Elvesier, Vol 8, 3,2003, 201–213.





[13] M.R. LEADBETTER, G. LINDGREN & H. ROOTZÉN, Extremes and related properties of randomsequences and processes, Springer Science & Business Media, 2012.





[14] J. PICKANDS III, Statistical inference using extreme order statistics, The Annals of Statistics,1995, 119–131.





[15] R CORE TEAM, R: A Language and Environment for Statistical Computing, R Foundation forStatistical Computing, Vienna, Austria, 2017, https://www.R-project.org.



# Index

arrival time, 13

cluster, 17

confidence interval, 10, 12, 21

declustering, 17

Delta method, 6, 10, 12

distribution

annual maximum, 4

Bernoulli, 14

binomial, 12, 14

conditional, 8, 11

excess, 7, 9, 10, 12

Fréchet, 2

generalized extreme value, 3, 4, 7, 9, 12, 14,16

generalized Pareto, 8–10, 12, 14, 17, 21

Gumbel, 2, 3, 5

max-stable, 2, 3

minima, 5

of maxima, 1

Poisson, 13

Weibull, 3, 21

domain of attraction, 2, 3

excess, 12, 17, 22

function

density, 5

intensity, 14, 15

return level, 12

intensity measure, 13

likelihood, 6, 10–12, 15

log-likelihood, 5, 11

natural estimator, 11

plot

box, 19

mean residual life, 9, 10, 21

quantile, 12

return level, 4, 12

scatter, 19, 20

process

homogeneous Poisson, 13

non-homogeneous Poisson, 13, 15

point, 13, 16

Poisson, 13–16

scaling point, 14

quantile, 4

return level, 4, 6, 11, 16, 21, 22

return period, 4

stationary series, 16, 17

theorem

Fisher - Tippett - Gnedenko, 2

GEV distribution function, 3

GEV distribution function for minima, 5

Pickands, Balkema and de Haan, 8

threshold, 7, 9–12, 17, 21

upper bound, 8, 22

upper-end point, 1, 14
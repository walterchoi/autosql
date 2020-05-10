 
            var number = new RegExp(/^-*[0-9]*$|^-*[0-9]{1,3}(,[0-9]{3})*$|^\.{1}$/gmi)
            var decimal = new RegExp(/^-*[0-9]*$|^-*[0-9]{1,3}(,[0-9]{3})*\.[0-9]*[^\D]$|^([-]{0,1}[0-9]*\.[0-9]*[^\D])/gmi)
            var exponential = new RegExp(/^([-]{0,1}[0-9]*e{1}[0-9]*[^\\D])$/gmi)
            var datetime = new RegExp(/^\/Date\([0-9]{13}(\+[0-9]{4}\)){0,1}\/$|^["]?([0-9]{4})(\/|-| |.)((((0{0,1}[13578]|10|12)(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9]|3[0-1]))|((0{0,1}[469]|11)(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9]|30))|(0{0,1}2(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9])))|(((0{0,1}[1-9]|[1-2][0-9]|3[0-1])(\/|-| |.)(0{0,1}[13578]|10|12))|((0{0,1}[1-9]|[1-2][0-9]|30)(\/|-| |.)(0{0,1}[469]|11))|((0{0,1}[1-9]|[1-2][0-9])(\/|-| |.)0{0,1}2)))[ |T]([0-1][0-9]|2[0-3])[:][0-5][0-9][:][0-5][0-9][.]?[0-9]{0,4}(["]?$|Z|\+[0-9]{0,3})["]?$|^["]?[a-z]{3} [a-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT[+]?[-]?[0-9]{4} [(][a-z ]*[)][ ]*["]?$|^([0]{0,1}[1-9]|[1-2][0-9]|3[0-1])(\/|-| |.)(0[1-9]|[1-2][0-9]|3[0-1])(\/|-| |.)([0-9]{4})[ |T]([0-1]{0,1}[0-9]|2[0-3])[:][0-5][0-9]([:][0-5][0-9]){0,1}[.]?[0-9]{0,4}(["]?$|Z|\+[0-9]{0,3})["]?$|^\/Date\([0-9]{13}(\+[0-9]{4}\)){0,1}\/$|^["]?([0-9]{4})(\/|-| |.)(((0{0,1}[13578]|10|12)(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9]|3[0-1]))|((0{0,1}[469]|11)(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9]|30))|(0{0,1}2(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9])))[ |T]([0-1][0-9]|2[0-3])([:][0-5][0-9]){0,2}[.]?[0-9]{0,4}(["]?$|Z|\+[0-9]{0,3})["]?$|^["]?[a-z]{3} [a-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT[+]?[-]?[0-9]{4} [(][a-z ]*[)][ ]*["]?$|^(([0]{0,1}[1-9]|[1-2][0-9]|3[0-1])(\/|-| |.)(0{0,1}[1-9]|[1-2][0-9]|3[0-1])(\/|-| |.)([0-9]{4})[ |T|-]([0-1]{0,1}[0-9]|2[0-3])([:][0-5][0-9]){0,2}[.]?[0-9]{0,4}(["]?$|Z|\+[0-9]{0,3})["]?)$/gmi)
            var date = new RegExp(/^(((0{0,1}[13578]|10|12)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]|3[0-1])|(0{0,1}[469]|11)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]|30)|(0{0,1}2)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]))|(((0{0,1}[1-9]|[1-2][0-9]|3[0-1])(\/|-| )((0{0,1}[13578])|10|12))|((0{0,1}[1-9]|[1-2][0-9]|30)(\/|-| )0{0,1}[469]|11)|((0{0,1}[1-9]|[1-2][0-9])(\/|-| )(0{0,1}2))))(\/|-| )([0-9]{4})$|^([0-9]{4})(\/|-| )(((0{0,1}[13578]|10|12)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]|3[0-1])|(0{0,1}[469]|11)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]|30)|(0{0,1}2)(\/|-| )(0{0,1}[1-9]|[1-2][0-9]))$|((0{0,1}[1-9]|[1-2][0-9]|3[0-1](\/|-| )(0{0,1}[13578])|10|12)|((0{0,1}[1-9]|[1-2][0-9]|30)(\/|-| )0{0,1}[469]|11)|((0{0,1}[1-9]|[1-2][0-9])(\/|-| )(0{0,1}2)))$)/gmi)
            var time = new RegExp(/^[-]*[0-9]{2,3}:[0-5][0-9]:[0-5][0-9]$/gmi)
            var json = new RegExp(/^{.+:.+}$/gmi)
            var binary = new RegExp(/^[0-1]*$/gmi)
            var boolean = new RegExp(/^[0-1]{1}$|^true$|^false$/gmi)

            module.exports = {
                number,
                decimal,
                exponential,
                datetime,
                date,
                time,
                json,
                binary,
                boolean
            }
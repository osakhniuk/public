let name = Property.string('Name', null, null, '');
let age = Property.int('Age', null, null, 30);
let height = Property.float('Height', null, null, 175.5);
var properties = [name, age, height];

let v = View.create();
v.name = 'JS View';
let content = ui.div();

var inputName = InputBase.forProperty(name);
var inputAge = InputBase.forProperty(age);
var inputHeight = InputBase.forProperty(height);
inputHeight.format = 'two digits after comma';
var inputs = [inputName, inputAge, inputHeight];

content.appendChild(inputName.root);
content.appendChild(inputAge.root);
content.appendChild(inputHeight.root);

for (let n = 0; n < inputs.length; n++) {
    let input = inputs[n];
    input.addCaption(properties[n].name);
    input.setTooltip(`Subject ${properties[n].name.toLowerCase()}`);
    input.nullable = false;
    input.fireChanged();
    let label = ui.divText('');
    content.appendChild(label);
    input.onChanged(function () {
        label.innerText = `${input.caption}: ${input.stringValue}`;
    });
}

var reset = function () {
    for (var input of inputs)
        input.value = properties.find((property) => property.name === input.caption).defaultValue;
}
reset();

content.appendChild(ui.bigButton('Post', function () {
    gr.balloon.info(inputs.map((input) => `${input.caption}: ${input.stringValue}`).join('<br>'));
}));
content.appendChild(ui.bigButton('Toggle enabled', function () {
    for (let input of inputs)
        input.enabled = !input.enabled;
}));
content.appendChild(ui.bigButton('Reset', reset));

v.root.appendChild(content);
gr.addView(v);